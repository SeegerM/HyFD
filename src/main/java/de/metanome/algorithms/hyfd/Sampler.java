package de.metanome.algorithms.hyfd;

import java.util.*;

import de.metanome.algorithms.hyfd.structures.FDList;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;
import de.metanome.algorithms.hyfd.structures.IntegerPair;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import de.metanome.algorithms.hyfd.utils.Logger;
import de.metanome.algorithms.hyfd.utils.ValueComparator;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class Sampler {

	private FDSet negCover;
	private FDTree posCover;
	private int[][] compressedRecords;
	private List<PositionListIndex> plis;
	private float efficiencyThreshold;
	private ValueComparator valueComparator;
	private List<AttributeRepresentant> attributeRepresentants = null;
	private PriorityQueue<AttributeRepresentant> queue = null;
	private MemoryGuardian memoryGuardian;
	private int maxViolations;

	private Map<BitSet, Integer> violationCounts2 = new HashMap<>();

	public Sampler(FDSet negCover, FDTree posCover, int maxViolations, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, ValueComparator valueComparator, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.compressedRecords = compressedRecords;
		this.plis = plis;
		this.efficiencyThreshold = efficiencyThreshold;
		this.valueComparator = valueComparator;
		this.memoryGuardian = memoryGuardian;
		this.maxViolations = maxViolations;
	}

	public FDList enrichNegativeCover(List<IntegerPair> comparisonSuggestions) {
		int numAttributes = this.compressedRecords[0].length;
		
		Logger.getInstance().writeln("Investigating comparison suggestions ... ");
		FDList newNonFds = new FDList(numAttributes, this.negCover.getMaxDepth());
		BitSet equalAttrs = new BitSet(this.posCover.getNumAttributes());
		Int2IntArrayMap violationCountMap = new Int2IntArrayMap();
		BitSet[] attributeViolations = new BitSet[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			attributeViolations[i] = new BitSet();
		}

		for (IntegerPair comparisonSuggestion : comparisonSuggestions) {
			// Update equalAttrs and per-attribute violation data.
			this.match(equalAttrs, comparisonSuggestion.a(), comparisonSuggestion.b(), violationCountMap, attributeViolations);
			// For suggestions, you might want to decide later how to use the per-attribute data.
			// (Here you could defer addition until after processing all suggestions.)
		}

		// Example: Process the accumulated violation data from suggestions.
		for (int i = 0; i < numAttributes; i++) {
			if (violationCountMap.get(i) >= this.maxViolations) {
				BitSet vioCandidate = attributeViolations[i];
				if (!this.negCover.contains(vioCandidate)) {
					BitSet candidateCopy = (BitSet) vioCandidate.clone();
					this.negCover.add(candidateCopy);
					newNonFds.add(candidateCopy);
					this.memoryGuardian.memoryChanged(1);
					this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
				}
			}
		}
		
		if (this.attributeRepresentants == null) { // if this is the first call of this method
			Logger.getInstance().write("Sorting clusters ...");
			long time = System.currentTimeMillis();
			ClusterComparator comparator = new ClusterComparator(this.compressedRecords, this.compressedRecords[0].length - 1, 1);
			for (PositionListIndex pli : this.plis) {
				for (IntArrayList cluster : pli.getClusters()) {
					Collections.sort(cluster, comparator);
				}
				comparator.incrementActiveKey();
			}
			Logger.getInstance().writeln("(" + (System.currentTimeMillis() - time) + "ms)");
		
			Logger.getInstance().write("Running initial windows ...");
			time = System.currentTimeMillis();
			this.attributeRepresentants = new ArrayList<AttributeRepresentant>(numAttributes);
			this.queue = new PriorityQueue<AttributeRepresentant>(numAttributes);
			for (int i = 0; i < numAttributes; i++) {
				AttributeRepresentant attributeRepresentant = new AttributeRepresentant(this.plis.get(i).getClusters(), this.negCover, this.posCover, this, this.memoryGuardian);
				attributeRepresentant.runNext(newNonFds, this.compressedRecords);
				this.attributeRepresentants.add(attributeRepresentant);
				if (attributeRepresentant.getEfficiency() > 0.0f)
					this.queue.add(attributeRepresentant); // If the efficiency is 0, the algorithm will never schedule a next run for the attribute regardless how low we set the efficiency threshold
			}
			
			if (!this.queue.isEmpty())
				this.efficiencyThreshold = Math.min(0.01f, this.queue.peek().getEfficiency() * 0.5f); // This is an optimization that we added after writing the HyFD paper
			
			Logger.getInstance().writeln("(" + (System.currentTimeMillis() - time) + "ms)");
		}
		else {
			// Decrease the efficiency threshold
			if (!this.queue.isEmpty())
				this.efficiencyThreshold = Math.min(this.efficiencyThreshold / 2, this.queue.peek().getEfficiency() * 0.9f); // This is an optimization that we added after writing the HyFD paper
		}
		
		Logger.getInstance().writeln("Moving window over clusters ... ");
		
		while (!this.queue.isEmpty() && (this.queue.peek().getEfficiency() >= this.efficiencyThreshold)) {
			AttributeRepresentant attributeRepresentant = this.queue.remove();
			
			attributeRepresentant.runNext(newNonFds, this.compressedRecords);
			
			if (attributeRepresentant.getEfficiency() > 0.0f)
				this.queue.add(attributeRepresentant);
		}
		
		StringBuilder windows = new StringBuilder("Window signature: ");
		for (AttributeRepresentant attributeRepresentant : this.attributeRepresentants)
			windows.append("[" + attributeRepresentant.windowDistance + "]");
		Logger.getInstance().writeln(windows.toString());
			
		return newNonFds;
	}

	private class ClusterComparator implements Comparator<Integer> {
		
		private int[][] sortKeys;
		private int activeKey1;
		private int activeKey2;
		
		public ClusterComparator(int[][] sortKeys, int activeKey1, int activeKey2) {
			super();
			this.sortKeys = sortKeys;
			this.activeKey1 = activeKey1;
			this.activeKey2 = activeKey2;
		}
		
		public void incrementActiveKey() {
			this.activeKey1 = this.increment(this.activeKey1);
			this.activeKey2 = this.increment(this.activeKey2);
		}
		
		@Override
		public int compare(Integer o1, Integer o2) {
			// Next
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey2];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey2];
			return value2 - value1;
		*/	
			// Previous
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey1];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey1];
			return value2 - value1;
		*/	
			// Previous -> Next
			int value1 = this.sortKeys[o1.intValue()][this.activeKey1];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey1];
			int result = value2 - value1;
			if (result == 0) {
				value1 = this.sortKeys[o1.intValue()][this.activeKey2];
				value2 = this.sortKeys[o2.intValue()][this.activeKey2];
			}
			return value2 - value1;
			
			// Next -> Previous
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey2];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey2];
			int result = value2 - value1;
			if (result == 0) {
				value1 = this.sortKeys[o1.intValue()][this.activeKey1];
				value2 = this.sortKeys[o2.intValue()][this.activeKey1];
			}
			return value2 - value1;
		*/	
		}
		
		private int increment(int number) {
			return (number == this.sortKeys[0].length - 1) ? 0 : number + 1;
		}
	}

	private class AttributeRepresentant implements Comparable<AttributeRepresentant> {
		
		private int windowDistance;
		private IntArrayList numNewNonFds = new IntArrayList();
		private IntArrayList numComparisons = new IntArrayList();
		private List<IntArrayList> clusters;
		private FDSet negCover;
		private FDTree posCover;
		private Sampler sampler;
		private MemoryGuardian memoryGuardian;

		private Map<BitSet, Integer> violationCounts = new HashMap<>();
		
		public float getEfficiency() {
			int index = this.numNewNonFds.size() - 1;
	/*		int sumNonFds = 0;
			int sumComparisons = 0;
			while ((index >= 0) && (sumComparisons < this.efficiencyFactor)) { //TODO: If we calculate the efficiency with all comparisons and all results in the log, then we can also aggregate all comparisons and results in two variables without maintaining the entire log
				sumNonFds += this.numNewNonFds.getInt(index);
				sumComparisons += this.numComparisons.getInt(index);
				index--;
			}
			if (sumComparisons == 0)
				return 0;
			return sumNonFds / sumComparisons;
	*/		float sumNewNonFds = this.numNewNonFds.getInt(index);
			float sumComparisons = this.numComparisons.getInt(index);
			if (sumComparisons == 0)
				return 0.0f;
			return sumNewNonFds / sumComparisons;
		}
		
		public AttributeRepresentant(List<IntArrayList> clusters, FDSet negCover, FDTree posCover, Sampler sampler, MemoryGuardian memoryGuardian) {
			this.clusters = new ArrayList<IntArrayList>(clusters);
			this.negCover = negCover;
			this.posCover = posCover;
			this.sampler = sampler;
			this.memoryGuardian = memoryGuardian;
		}
		
		@Override
		public int compareTo(AttributeRepresentant o) {
//			return o.getNumNewNonFds() - this.getNumNewNonFds();		
			return (int)Math.signum(o.getEfficiency() - this.getEfficiency());
		}
		
		public void runNext(FDList newNonFds, int[][] compressedRecords) {
			this.windowDistance++;
			int numNewNonFds = 0;
			int numComparisons = 0;
			int numAttributes = this.posCover.getNumAttributes();
			BitSet equalAttrs = new BitSet(numAttributes);
			
			int previousNegCoverSize = newNonFds.size();
			Iterator<IntArrayList> clusterIterator = this.clusters.iterator();
			Int2IntArrayMap violationCountMap = new Int2IntArrayMap();
			BitSet[] attributeViolations = new BitSet[numAttributes];
			for (int i = 0; i < numAttributes; i++) {
				attributeViolations[i] = new BitSet();
			}

			while (clusterIterator.hasNext()) {
				IntArrayList cluster = clusterIterator.next();
				
				if (cluster.size() <= this.windowDistance) {
					clusterIterator.remove();
					continue;
				}

				for (int recordIndex = 0; recordIndex < (cluster.size() - this.windowDistance); recordIndex++) {
					int recordId = cluster.getInt(recordIndex);
					int partnerRecordId = cluster.getInt(recordIndex + this.windowDistance);
					
					this.sampler.match(equalAttrs, compressedRecords[recordId], compressedRecords[partnerRecordId], violationCountMap , attributeViolations);

					/*if (!this.negCover.contains(equalAttrs)) {
						BitSet equalAttrsCopy = (BitSet) equalAttrs.clone();
						this.negCover.add(equalAttrsCopy);
						newNonFds.add(equalAttrsCopy);

						this.memoryGuardian.memoryChanged(1);
						this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
					}

					 */
					// Record the violation (increment count and add candidate to negCover only if count exceeds maxViolations)
					//this.recordViolation(equalAttrs, newNonFds);

					numComparisons++;
				}
			}
			for (int i = 0; i < numAttributes; i++) {
				if (violationCountMap.get(i) > this.sampler.maxViolations){
					if (attributeViolations[i] != null && !this.negCover.contains(attributeViolations[i])) {
						BitSet candidateCopy = (BitSet) attributeViolations[i].clone();
						this.negCover.add(candidateCopy);
						newNonFds.add(candidateCopy);
						this.memoryGuardian.memoryChanged(1);
						this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
					}
				}
			}
			numNewNonFds = newNonFds.size() - previousNegCoverSize;
			
			this.numNewNonFds.add(numNewNonFds);
			this.numComparisons.add(numComparisons);
		}

		/**
		 * Records a violation for the candidate FD (represented by candidate).
		 * Only if the candidate has been violated more than maxViolations will it be added to the negative cover.
		 */
		private void recordViolation(BitSet candidate, FDList newNonFds) {
			// Clone the candidate to be used as a key (BitSet equality/hashCode is content-based)
			//BitSet key = (BitSet) candidate.clone();
			// If this candidate is already in the negative cover, we do nothing.
			if (this.negCover.contains(candidate))
				return;
			int count = this.violationCounts.getOrDefault(candidate, 0);
			count++;
			this.violationCounts.put(candidate, count);
			// Only add the candidate to the negative cover if the violation count exceeds maxViolations.
			if (count > this.sampler.maxViolations) {
				BitSet keyCopy = (BitSet) candidate.clone();
				this.negCover.add(keyCopy);
				newNonFds.add(keyCopy);
				this.memoryGuardian.memoryChanged(1);
				this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
			}
		}
	}
	
	private void match(BitSet equalAttrs, int t1, int t2, Int2IntArrayMap map, BitSet[] violations) {
		this.match(equalAttrs, this.compressedRecords[t1], this.compressedRecords[t2], map, violations);
	}
	
	private void match(BitSet equalAttrs, int[] t1, int[] t2, Int2IntArrayMap violationCountMap, BitSet[] violations) {
		equalAttrs.clear(0, t1.length);
		int n = t1.length;

		// First, compute equalAttrs: set bits for attributes where t1 and t2 are equal.
		for (int i = 0; i < n; i++) {
			if (this.valueComparator.isEqual(t1[i], t2[i])) {
				equalAttrs.set(i);
			}
		}

		// Then, for each attribute where t1 and t2 differ,
		// increment its violation count and accumulate the candidate FD (equalAttrs)
		// into the corresponding violation BitSet.
		for (int i = 0; i < n; i++) {
			if (!this.valueComparator.isEqual(t1[i], t2[i])) {
				int currentCount = violationCountMap.get(i);
				violationCountMap.put(i, currentCount + 1);
				// Accumulate the candidate FD for attribute i by OR-ing the complete equalAttrs.
				violations[i].or(equalAttrs);
			}
		}
	}

	private void recordViolation(BitSet candidate, FDList newNonFds) {
		// Clone the candidate to be used as a key (BitSet equality/hashCode is content-based)
		//BitSet key = (BitSet) candidate.clone();
		// If this candidate is already in the negative cover, we do nothing.
		if (this.negCover.contains(candidate))
			return;
		int count = violationCounts2.getOrDefault(candidate, 0);
		count++;
		violationCounts2.put(candidate, count);
		// Only add the candidate to the negative cover if the violation count exceeds maxViolations.
		if (count > this.maxViolations) {
			BitSet keyCopy = (BitSet) candidate.clone();
			this.negCover.add(keyCopy);
			newNonFds.add(keyCopy);
			this.memoryGuardian.memoryChanged(1);
			this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
		}
	}
}
