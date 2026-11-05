package de.metanome.algorithms.hyfd.structures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Prototype: proofs that partial FDs are invalid, reusable across candidates.
 *
 * A certificate for a rhs attribute A is a set of record pairs that pairwise share no record and that each disagree on A.
 * The pairs are aggregated by their difference set D, i.e., the attributes on which the two records differ.
 * A pair violates X->A iff the records agree on X, i.e., iff X does not intersect D. Because the pairs share no records,
 * any set of records whose removal makes X->A hold must contain at least one record of every violating pair. Hence
 * g3(X->A) >= the total weight of all difference sets that X misses, and if that weight exceeds maxViolations, X->A is
 * provably invalid. Certificates never prove validity; candidates that they cannot reject are validated as usual.
 *
 * Building certificates costs time, so each rhs attribute gets a budget of new certificates per lattice level; when the
 * store for a rhs is full, a new certificate replaces the one that rejected the fewest candidates so far.
 */
public class ViolationCertificates {

	public static boolean isEnabled() {
		return Boolean.getBoolean("hyfd.certificates");
	}

	public static class Certificate {
		private final BitSet[] differenceSets;	// sorted by descending weight
		private final int[] weights;
		private final int totalWeight;
		private final AtomicInteger proofs = new AtomicInteger();

		private Certificate(BitSet[] differenceSets, int[] weights, int totalWeight) {
			this.differenceSets = differenceSets;
			this.weights = weights;
			this.totalWeight = totalWeight;
		}
	}

	private final int maxViolations;
	private final int maxCertificatesPerRhs;
	private final int buildsPerRhsAndLevel;
	private final AtomicReferenceArray<Certificate[]> certificates; // per rhs attribute; arrays are replaced, never modified
	private final AtomicIntegerArray builds;						// per rhs attribute; builds in the current level

	private final AtomicLong checks = new AtomicLong();
	private final AtomicLong proofs = new AtomicLong();
	private final AtomicLong added = new AtomicLong();

	public ViolationCertificates(int numAttributes, int maxViolations, int maxCertificatesPerRhs, int buildsPerRhsAndLevel) {
		this.maxViolations = maxViolations;
		this.maxCertificatesPerRhs = maxCertificatesPerRhs;
		this.buildsPerRhsAndLevel = buildsPerRhsAndLevel;
		this.certificates = new AtomicReferenceArray<>(numAttributes);
		this.builds = new AtomicIntegerArray(numAttributes);
		for (int attr = 0; attr < numAttributes; attr++)
			this.certificates.set(attr, new Certificate[0]);
	}

	/**
	 * Resets the build budgets; to be called before each lattice level.
	 */
	public void startLevel() {
		for (int attr = 0; attr < this.builds.length(); attr++)
			this.builds.set(attr, 0);
	}

	/**
	 * @return true if new certificates for this rhs may still be built in the current level (cheap pre-check for collecting pairs)
	 */
	public boolean wantsCertificate(int rhs) {
		return this.builds.get(rhs) < this.buildsPerRhsAndLevel;
	}

	/**
	 * @return true if some certificate proves that lhs->rhs has more than maxViolations violations (g3)
	 */
	public boolean provesInvalid(BitSet lhs, int rhs) {
		this.checks.incrementAndGet();
		for (Certificate certificate : this.certificates.get(rhs)) {
			int missed = 0;
			int remaining = certificate.totalWeight;
			for (int i = 0; i < certificate.differenceSets.length; i++) {
				remaining -= certificate.weights[i];
				if (!certificate.differenceSets[i].intersects(lhs)) {
					missed += certificate.weights[i];
					if (missed > this.maxViolations) {
						certificate.proofs.incrementAndGet();
						this.proofs.incrementAndGet();
						return true;
					}
				}
				else if (missed + remaining <= this.maxViolations) {
					break; // This certificate cannot reach the bound anymore
				}
			}
		}
		return false;
	}

	/**
	 * Builds a certificate from the pairs that the collector gathered for rhs and stores it, if the build budget allows.
	 */
	public void addFrom(Collector collector, int rhs) {
		if (!collector.hasPairs(rhs) || (this.builds.getAndIncrement(rhs) >= this.buildsPerRhsAndLevel))
			return;
		this.add(rhs, collector.build(rhs));
	}

	private synchronized void add(int rhs, Certificate certificate) {
		if (certificate.totalWeight <= this.maxViolations)
			return; // Such a certificate cannot prove anything

		Certificate[] current = this.certificates.get(rhs);
		Certificate[] next;
		if (current.length < this.maxCertificatesPerRhs) {
			next = Arrays.copyOf(current, current.length + 1);
			next[current.length] = certificate;
		}
		else {
			int leastUsed = 0;
			for (int i = 1; i < current.length; i++)
				if (current[i].proofs.get() < current[leastUsed].proofs.get())
					leastUsed = i;
			next = current.clone();
			next[leastUsed] = certificate;
		}
		this.certificates.set(rhs, next);
		this.added.incrementAndGet();
	}

	public String getStatistics() {
		return "Certificates: " + this.added.get() + " added; " + this.checks.get() + " checks; " + this.proofs.get() + " candidates rejected without validation";
	}

	/**
	 * Collects record pairs that violate candidate FDs during one validation task (not thread-safe; one per task).
	 * Within one call of collect() the pairs are record-disjoint; groups passed for the same rhs must be disjoint
	 * record sets (which holds for the clusters/subclusters of one validation), so all pairs for a rhs are record-disjoint.
	 */
	public static class Collector {

		private final ViolationCertificates store;
		private final int[][] compressedRecords;
		private final int maxPairs;
		private final Int2ObjectOpenHashMap<IntArrayList> pairs = new Int2ObjectOpenHashMap<>(); // rhs -> (a0, b0, a1, b1, ...)
		private Int2IntOpenHashMap frequencies = null;

		public Collector(ViolationCertificates store, int[][] compressedRecords, int maxPairs) {
			this.store = store;
			this.compressedRecords = compressedRecords;
			this.maxPairs = maxPairs;
		}

		public boolean hasPairs(int rhsAttr) {
			IntArrayList rhsPairs = this.pairs.get(rhsAttr);
			return (rhsPairs != null) && !rhsPairs.isEmpty();
		}

		/**
		 * Like collect(rhsAttr, group, reference), but determines the group's most frequent rhs value itself.
		 */
		public void collect(int rhsAttr, IntArrayList group) {
			if (!this.store.wantsCertificate(rhsAttr))
				return;
			if (this.frequencies == null)
				this.frequencies = new Int2IntOpenHashMap();
			this.frequencies.clear();
			int reference = -1;
			int maxCount = 0;
			for (int i = 0; i < group.size(); i++) {
				int value = this.compressedRecords[group.getInt(i)][rhsAttr];
				if (value < 0)
					continue;
				int count = this.frequencies.addTo(value, 1) + 1;
				if (count > maxCount) {
					maxCount = count;
					reference = value;
				}
			}
			this.collect(rhsAttr, group, reference);
		}

		/**
		 * Adds record-disjoint pairs of records from the group that disagree on rhsAttr: each record without the reference
		 * (most frequent) rhs value is paired with a distinct record that has it; leftover records are paired with each other
		 * where their values differ. A rhs value of -1 is unique in its column and, hence, differs from all other values.
		 */
		public void collect(int rhsAttr, IntArrayList group, int reference) {
			if (!this.store.wantsCertificate(rhsAttr))
				return;
			IntArrayList rhsPairs = this.pairs.get(rhsAttr);
			if (rhsPairs == null) {
				rhsPairs = new IntArrayList();
				this.pairs.put(rhsAttr, rhsPairs);
			}
			int size = group.size();

			if (reference < 0) { // All rhs values in this group are unique, so any two records disagree
				for (int i = 0; i + 1 < size; i += 2)
					if (!this.addPair(rhsPairs, group.getInt(i), group.getInt(i + 1)))
						return;
				return;
			}

			int nextMajority = 0;
			int pendingMinority = -1;
			for (int i = 0; i < size; i++) {
				int record = group.getInt(i);
				int value = this.compressedRecords[record][rhsAttr];
				if (value == reference)
					continue;
				while ((nextMajority < size) && (this.compressedRecords[group.getInt(nextMajority)][rhsAttr] != reference))
					nextMajority++;
				if (nextMajority < size) {
					if (!this.addPair(rhsPairs, record, group.getInt(nextMajority)))
						return;
					nextMajority++;
				}
				else if (pendingMinority < 0) {
					pendingMinority = record;
				}
				else {
					int pendingValue = this.compressedRecords[pendingMinority][rhsAttr];
					if ((pendingValue < 0) || (value < 0) || (pendingValue != value)) {
						if (!this.addPair(rhsPairs, pendingMinority, record))
							return;
						pendingMinority = -1;
					}
					else {
						pendingMinority = record;
					}
				}
			}
		}

		private boolean addPair(IntArrayList rhsPairs, int a, int b) {
			if (rhsPairs.size() / 2 >= this.maxPairs)
				return false;
			rhsPairs.add(a);
			rhsPairs.add(b);
			return true;
		}

		private Certificate build(int rhsAttr) {
			IntArrayList rhsPairs = this.pairs.get(rhsAttr);
			int numAttributes = this.compressedRecords[0].length;
			Map<BitSet, int[]> weights = new HashMap<>();
			for (int i = 0; i < rhsPairs.size(); i += 2) {
				int[] recordA = this.compressedRecords[rhsPairs.getInt(i)];
				int[] recordB = this.compressedRecords[rhsPairs.getInt(i + 1)];
				BitSet differenceSet = new BitSet(numAttributes);
				for (int attr = 0; attr < numAttributes; attr++)
					if ((recordA[attr] < 0) || (recordB[attr] < 0) || (recordA[attr] != recordB[attr]))
						differenceSet.set(attr);
				int[] weight = weights.get(differenceSet);
				if (weight == null)
					weights.put(differenceSet, new int[] { 1 });
				else
					weight[0]++;
			}

			List<Map.Entry<BitSet, int[]>> entries = new ArrayList<>(weights.entrySet());
			entries.sort((e1, e2) -> Integer.compare(e2.getValue()[0], e1.getValue()[0]));
			BitSet[] differenceSets = new BitSet[entries.size()];
			int[] entryWeights = new int[entries.size()];
			int total = 0;
			for (int i = 0; i < entries.size(); i++) {
				differenceSets[i] = entries.get(i).getKey();
				entryWeights[i] = entries.get(i).getValue()[0];
				total += entryWeights[i];
			}
			return new Certificate(differenceSets, entryWeights, total);
		}
	}
}
