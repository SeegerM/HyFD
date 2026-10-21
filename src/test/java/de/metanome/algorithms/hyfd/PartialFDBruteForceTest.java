package de.metanome.algorithms.hyfd;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.RoundingMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.RelaxedFunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;

/**
 * Compares HyFD's partial FD discovery with a brute-force enumeration on many small random tables.
 * An FD X->A is valid if its g3 error (the number of records that must be removed for X->A to hold exactly)
 * is at most maxViolations, and minimal if no proper subset of X also determines A.
 */
public class PartialFDBruteForceTest {

	private static final int TRIALS_PER_THRESHOLD = 200;
	private static final int MAX_REPORTED_PROBLEMS = 20;

	@Test
	public void testExact() throws Exception {
		this.compareWithBruteForce(1.0f, 1);
	}

	@Test
	public void testThreshold095() throws Exception {
		this.compareWithBruteForce(0.95f, 2);
	}

	@Test
	public void testThreshold09() throws Exception {
		this.compareWithBruteForce(0.9f, 3);
	}

	@Test
	public void testThreshold08() throws Exception {
		this.compareWithBruteForce(0.8f, 4);
	}

	@Test
	public void testThreshold07() throws Exception {
		this.compareWithBruteForce(0.7f, 5);
	}

	@Test
	public void testThreshold06() throws Exception {
		this.compareWithBruteForce(0.6f, 6);
	}

	@Test
	public void testThreshold095MaxLhsSize2() throws Exception {
		this.compareWithBruteForce(0.95f, 2, 7);
	}

	@Test
	public void testThreshold08MaxLhsSize2() throws Exception {
		this.compareWithBruteForce(0.8f, 2, 8);
	}

	@Test
	public void testThreshold08MaxLhsSize1() throws Exception {
		this.compareWithBruteForce(0.8f, 1, 9);
	}

	@Test
	public void testColumnOrderInvariance() throws Exception {
		// The discovered FDs and their scores must not depend on the order of the columns in the input
		Random random = new Random(10);
		List<String> problems = new ArrayList<>();
		float[] thresholds = { 1.0f, 0.95f, 0.9f, 0.8f };
		int[] maxLhsSizes = { -1, 2 };
		for (int trial = 0; trial < TRIALS_PER_THRESHOLD; trial++) {
			List<List<String>> rows = randomTable(random, 6 + random.nextInt(300), 3 + random.nextInt(6));
			float threshold = thresholds[trial % thresholds.length];
			int maxLhsSize = maxLhsSizes[(trial / thresholds.length) % maxLhsSizes.length];
			int numAttributes = rows.get(0).size();

			List<String> columnNames = new ArrayList<>();
			for (int attr = 0; attr < numAttributes; attr++)
				columnNames.add("c" + attr);
			Map<String, Double> original = canonicalize(runHyFD(rows, columnNames, threshold, maxLhsSize), columnNames);

			List<Integer> permutation = new ArrayList<>();
			for (int attr = 0; attr < numAttributes; attr++)
				permutation.add(attr);
			Collections.shuffle(permutation, random);
			List<String> permutedNames = new ArrayList<>();
			for (int attr : permutation)
				permutedNames.add(columnNames.get(attr));
			List<List<String>> permutedRows = new ArrayList<>();
			for (List<String> row : rows) {
				List<String> permutedRow = new ArrayList<>();
				for (int attr : permutation)
					permutedRow.add(row.get(attr));
				permutedRows.add(permutedRow);
			}
			Map<String, Double> permuted = canonicalize(runHyFD(permutedRows, permutedNames, threshold, maxLhsSize), permutedNames);

			if (!original.equals(permuted))
				problems.add("trial " + trial + " (" + rows.size() + " rows, " + numAttributes + " columns, threshold " + threshold + ", maxLhsSize " + maxLhsSize
						+ ", column order " + permutedNames + "): " + original.size() + " FDs in original order, " + permuted.size() + " FDs in permuted order");
		}
		assertTrue(problems.size() + " order-dependent results, e.g.:\n" + String.join("\n", problems.subList(0, Math.min(problems.size(), MAX_REPORTED_PROBLEMS))),
				problems.isEmpty());
	}

	@Test
	public void testSingleColumn() throws Exception {
		// The sampler's cluster sorting must not assume a second column
		List<List<String>> rows = new ArrayList<>();
		for (String value : new String[] { "a", "a", "a", "b", "b" })
			rows.add(new ArrayList<>(Collections.singletonList(value)));
		for (float threshold : new float[] { 1.0f, 0.6f }) {
			List<String> problems = this.compare(rows, threshold, -1);
			assertTrue(String.join("\n", problems), problems.isEmpty());
		}
	}

	@Test
	public void testMaxViolationsWithoutFloatRounding() throws Exception {
		// 25 * (1 - 0.6) = 10 allowed violations; single-precision float arithmetic yields 9.999999 and truncates it to 9
		List<List<String>> rows = new ArrayList<>();
		for (int r = 0; r < 25; r++) {
			List<String> row = new ArrayList<>();
			Collections.addAll(row, (r < 15) ? "a" : "b" + r, String.valueOf(r));
			rows.add(row);
		}
		List<String> problems = this.compare(rows, 0.6f, -1); // [] -> c0 has g3 = 10
		assertTrue(String.join("\n", problems), problems.isEmpty());
	}

	@Test
	public void testInvalidThresholdsAreRejected() throws Exception {
		for (String threshold : new String[] { "-0.1", "1.5", "abc" }) {
			try {
				new HyFD().setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), threshold);
				fail("Threshold " + threshold + " was accepted");
			}
			catch (AlgorithmConfigurationException e) {
				// expected
			}
		}
	}

	@Test
	public void testUniqueRhsValuesAreNotEqual() throws Exception {
		// c2 is unique in every record, so [c0,c1]->c2 has g3 = 6 (one record per lhs group can be kept), which exceeds maxViolations = 2
		List<List<String>> rows = new ArrayList<>();
		String[] c1Values = { "x", "x", "y", "y", "z", "z" };
		for (int r = 0; r < 12; r++) {
			List<String> row = new ArrayList<>();
			Collections.addAll(row, (r < 6) ? "a" : "b", c1Values[r % 6], String.valueOf(r));
			rows.add(row);
		}
		List<String> problems = this.compare(rows, 0.8f, -1); // maxViolations = 2
		assertTrue(String.join("\n", problems), problems.isEmpty());
	}

	private void compareWithBruteForce(float threshold, long seed) throws Exception {
		this.compareWithBruteForce(threshold, -1, seed);
	}

	private void compareWithBruteForce(float threshold, int maxLhsSize, long seed) throws Exception {
		Random random = new Random(seed);
		List<String> problems = new ArrayList<>();
		for (int trial = 0; trial < TRIALS_PER_THRESHOLD; trial++) {
			List<List<String>> rows = randomTable(random, 6 + random.nextInt(40), 3 + random.nextInt(4));
			for (String problem : this.compare(rows, threshold, maxLhsSize))
				problems.add("trial " + trial + " (" + rows.size() + " rows, " + rows.get(0).size() + " columns): " + problem);
		}
		assertTrue(problems.size() + " problems for threshold " + threshold + " and maxLhsSize " + maxLhsSize + ", e.g.:\n" + String.join("\n", problems.subList(0, Math.min(problems.size(), MAX_REPORTED_PROBLEMS))),
				problems.isEmpty());
	}

	/** Random table whose later columns are often noisy functions of earlier ones, so that many partial FDs exist. */
	private static List<List<String>> randomTable(Random random, int numRecords, int numAttributes) {
		int[] domainSizes = new int[numAttributes];
		for (int attr = 0; attr < numAttributes; attr++)
			domainSizes[attr] = 1 + random.nextInt(random.nextInt(4) == 0 ? numRecords : 5);

		List<List<String>> rows = new ArrayList<>();
		for (int r = 0; r < numRecords; r++) {
			List<String> row = new ArrayList<>();
			for (int attr = 0; attr < numAttributes; attr++) {
				if ((attr >= 2) && (random.nextInt(3) == 0) && (random.nextInt(10) != 0))
					row.add("f" + ((row.get(0).hashCode() * 31 + row.get(attr - 1).hashCode()) & 3));
				else
					row.add("v" + random.nextInt(domainSizes[attr]));
			}
			rows.add(row);
		}
		return rows;
	}

	private List<String> compare(List<List<String>> rows, float threshold, int maxLhsSize) throws Exception {
		int numRecords = rows.size();
		int numAttributes = rows.get(0).size();
		List<String> columnNames = new ArrayList<>();
		for (int attr = 0; attr < numAttributes; attr++)
			columnNames.add("c" + attr);
		int maxViolations = maxViolations(numRecords, threshold);

		Map<String, Integer> expected = bruteForce(rows, maxViolations, maxLhsSize);
		Map<String, Double> actual = runHyFD(rows, columnNames, threshold, maxLhsSize);

		List<String> problems = new ArrayList<>();
		for (Map.Entry<String, Integer> fd : expected.entrySet()) {
			if (!actual.containsKey(fd.getKey())) {
				problems.add("missing " + pretty(fd.getKey(), columnNames) + " (g3=" + fd.getValue() + ", maxViolations=" + maxViolations + ")");
				continue;
			}
			double expectedScore = 1d - (double) fd.getValue() / numRecords;
			double actualScore = actual.get(fd.getKey());
			if (Math.abs(actualScore - expectedScore) > 1e-4)
				problems.add("wrong score for " + pretty(fd.getKey(), columnNames) + ": " + actualScore + " instead of " + expectedScore);
		}
		for (String fd : actual.keySet()) {
			if (expected.containsKey(fd))
				continue;
			String[] parts = fd.split("->");
			int error = g3(rows, Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
			if (error > maxViolations)
				problems.add("invalid " + pretty(fd, columnNames) + " (g3=" + error + ", maxViolations=" + maxViolations + ")");
			else
				problems.add("non-minimal " + pretty(fd, columnNames));
		}
		return problems;
	}

	/** floor(numRecords * (1 - threshold)) in exact decimal arithmetic. */
	private static int maxViolations(int numRecords, float threshold) {
		return BigDecimal.valueOf(numRecords).multiply(BigDecimal.ONE.subtract(new BigDecimal(String.valueOf(threshold)))).setScale(0, RoundingMode.FLOOR).intValueExact();
	}

	private static Map<String, Double> runHyFD(List<List<String>> rows, final List<String> columnNames, float threshold, int maxLhsSize) throws Exception {
		final Map<String, Double> results = new TreeMap<>();
		HyFD hyFD = new HyFD();
		hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), new MemoryInputGenerator(rows, columnNames));
		hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), String.valueOf(threshold));
		hyFD.setBooleanConfigurationValue(HyFD.Identifier.VALIDATE_PARALLEL.name(), false);
		hyFD.setBooleanConfigurationValue(HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name(), false);
		hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), maxLhsSize);
		hyFD.setResultReceiver(new RelaxedFunctionalDependencyResultReceiver() {
			@Override
			public void receiveResult(RelaxedFunctionalDependency fd) {
				int lhs = 0;
				for (ColumnIdentifier column : fd.getDeterminant().getColumnIdentifiers())
					lhs |= 1 << columnNames.indexOf(column.getColumnIdentifier());
				results.put(key(lhs, columnNames.indexOf(fd.getDependant().getColumnIdentifier())), fd.getMeasure());
			}

			@Override
			public Boolean acceptedResult(RelaxedFunctionalDependency fd) {
				return Boolean.TRUE;
			}
		});
		hyFD.execute();
		return results;
	}

	/** All minimal FDs with g3 <= maxViolations and at most maxLhsSize lhs attributes (-1 = unlimited), mapped to their g3 error. */
	private static Map<String, Integer> bruteForce(List<List<String>> rows, int maxViolations, int maxLhsSize) {
		int numAttributes = rows.get(0).size();
		Map<String, Integer> fds = new TreeMap<>();
		for (int rhs = 0; rhs < numAttributes; rhs++) {
			int[] errors = new int[1 << numAttributes];
			for (int lhs = 0; lhs < (1 << numAttributes); lhs++)
				if ((lhs & (1 << rhs)) == 0)
					errors[lhs] = g3(rows, lhs, rhs);

			for (int lhs = 0; lhs < (1 << numAttributes); lhs++) {
				if (((lhs & (1 << rhs)) != 0) || (errors[lhs] > maxViolations) || ((maxLhsSize >= 0) && (Integer.bitCount(lhs) > maxLhsSize)))
					continue;
				boolean minimal = true; // g3 can only grow when removing lhs attributes, so checking direct subsets suffices
				for (int attr = 0; attr < numAttributes; attr++)
					if (((lhs & (1 << attr)) != 0) && (errors[lhs & ~(1 << attr)] <= maxViolations))
						minimal = false;
				if (minimal)
					fds.put(key(lhs, rhs), errors[lhs]);
			}
		}
		return fds;
	}

	private static int g3(List<List<String>> rows, int lhs, int rhs) {
		Map<List<String>, Map<String, Integer>> groups = new HashMap<>();
		for (List<String> row : rows) {
			List<String> lhsValues = new ArrayList<>();
			for (int attr = 0; attr < row.size(); attr++)
				if ((lhs & (1 << attr)) != 0)
					lhsValues.add(row.get(attr));
			Map<String, Integer> rhsFrequencies = groups.get(lhsValues);
			if (rhsFrequencies == null)
				groups.put(lhsValues, rhsFrequencies = new HashMap<>());
			rhsFrequencies.merge(row.get(rhs), 1, Integer::sum);
		}
		int kept = 0;
		for (Map<String, Integer> rhsFrequencies : groups.values())
			kept += Collections.max(rhsFrequencies.values());
		return rows.size() - kept;
	}

	private static String key(int lhs, int rhs) {
		return lhs + "->" + rhs;
	}

	/** Re-keys results by column names with a sorted lhs, so that results for different column orders are comparable. */
	private static Map<String, Double> canonicalize(Map<String, Double> results, List<String> columnNames) {
		Map<String, Double> canonical = new TreeMap<>();
		for (Map.Entry<String, Double> fd : results.entrySet()) {
			String[] parts = fd.getKey().split("->");
			int lhs = Integer.parseInt(parts[0]);
			List<String> lhsNames = new ArrayList<>();
			for (int attr = 0; attr < columnNames.size(); attr++)
				if ((lhs & (1 << attr)) != 0)
					lhsNames.add(columnNames.get(attr));
			Collections.sort(lhsNames);
			canonical.put(lhsNames + " -> " + columnNames.get(Integer.parseInt(parts[1])), fd.getValue());
		}
		return canonical;
	}

	private static String pretty(String key, List<String> columnNames) {
		String[] parts = key.split("->");
		int lhs = Integer.parseInt(parts[0]);
		List<String> lhsNames = new ArrayList<>();
		for (int attr = 0; attr < columnNames.size(); attr++)
			if ((lhs & (1 << attr)) != 0)
				lhsNames.add(columnNames.get(attr));
		return lhsNames + " -> " + columnNames.get(Integer.parseInt(parts[1]));
	}

	private static class MemoryInputGenerator implements RelationalInputGenerator {
		private final List<List<String>> rows;
		private final List<String> columnNames;

		public MemoryInputGenerator(List<List<String>> rows, List<String> columnNames) {
			this.rows = rows;
			this.columnNames = columnNames;
		}

		@Override
		public RelationalInput generateNewCopy() {
			return new RelationalInput() {
				private int next = 0;

				@Override
				public boolean hasNext() {
					return this.next < rows.size();
				}

				@Override
				public List<String> next() {
					return rows.get(this.next++);
				}

				@Override
				public int numberOfColumns() {
					return columnNames.size();
				}

				@Override
				public String relationName() {
					return "random";
				}

				@Override
				public List<String> columnNames() {
					return columnNames;
				}

				@Override
				public void close() {
				}
			};
		}

		@Override
		public void close() {
		}
	}
}
