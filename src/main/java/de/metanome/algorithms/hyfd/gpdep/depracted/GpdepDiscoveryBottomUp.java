package de.metanome.algorithms.hyfd.gpdep.depracted;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GpdepDiscoveryBottomUp {

    private static final double EPS = 1e-12;

    private final int numAttributes;
    private final int numRecords;
    private final int[][] compressedRecords;
    private final List<PositionListIndex> plis;
    private final ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private final int maxLhsSize;
    private final double minGpdep;

    private final int[][] valuesByAttr;
    private final double[] rhoByAttr;

    private final int numThreads;

    public GpdepDiscoveryBottomUp(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep
    ) {
        this.numAttributes = numAttributes;
        this.numRecords = numRecords;
        this.compressedRecords = compressedRecords;
        this.plis = plis;
        this.columnIdentifiers = columnIdentifiers;
        this.maxLhsSize = maxLhsSize;
        this.minGpdep = minGpdep;

        this.numThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

        this.valuesByAttr = precomputeValueIds();
        this.rhoByAttr = precomputeRhos();
    }

    public List<ScoredFD> discoverParallel() {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<Future<List<ScoredFD>>> futures = new ArrayList<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            final int rhsAttr = rhs;

            futures.add(executor.submit(() -> discoverForRhs(rhsAttr)));
        }

        List<ScoredFD> allResults = new ArrayList<>();

        for (Future<List<ScoredFD>> future : futures) {
            try {
                allResults.addAll(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Gpdep discovery interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Gpdep discovery failed", e);
            }
        }

        executor.shutdown();

        allResults.sort(
                Comparator.comparingDouble((ScoredFD fd) -> fd.gpdep).reversed()
                        .thenComparingInt(fd -> fd.lhs.cardinality())
        );

        return allResults;
    }

    public List<ScoredFD> discover() {
        List<ScoredFD> allResults = new ArrayList<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            allResults.addAll(discoverForRhs(rhs));
        }

        allResults.sort(
                Comparator.comparingDouble((ScoredFD fd) -> fd.gpdep).reversed()
                        .thenComparingInt(fd -> fd.lhs.cardinality())
        );

        return allResults;
    }

    private List<ScoredFD> discoverForRhs(int rhs) {
        List<ScoredFD> results = new ArrayList<>();
        List<BitSet> exactMinimalLhss = new ArrayList<>();

        int[] rootGroups = new int[numRecords];
        Arrays.fill(rootGroups, 0);

        List<SearchNode> currentLevel = new ArrayList<>();
        currentLevel.add(new SearchNode(new BitSet(numAttributes), rootGroups, 1, -1));

        while (!currentLevel.isEmpty()) {
            List<SearchNode> nextLevel = new ArrayList<>();

            for (SearchNode node : currentLevel) {
                BitSet lhs = node.lhs;

                if (lhs.get(rhs)) {
                    continue;
                }

                if (containsProperSubset(lhs, exactMinimalLhss)) {
                    continue;
                }

                ScoredFD scored = scoreFromPartition(lhs, rhs, node.groupIds, node.groupCount);

                if (scored.gpdep + EPS >= minGpdep) {
                    addIfNonDominated(results, scored);
                }

                if (scored.exact) {
                    addExactMinimal(exactMinimalLhss, lhs);
                    continue;
                }

                if (lhs.cardinality() >= maxLhsSize) {
                    continue;
                }

                double upperBoundForDescendants = Math.max(1.0 - scored.epdep, 0.0);

                if (upperBoundForDescendants + EPS < minGpdep) {
                    continue;
                }

                for (int extensionAttr = node.lastAddedAttr + 1;
                     extensionAttr < numAttributes;
                     extensionAttr++) {

                    if (extensionAttr == rhs) {
                        continue;
                    }

                    BitSet childLhs = (BitSet) lhs.clone();
                    childLhs.set(extensionAttr);

                    Partition childPartition = refinePartition(
                            node.groupIds,
                            node.groupCount,
                            extensionAttr
                    );

                    nextLevel.add(
                            new SearchNode(
                                    childLhs,
                                    childPartition.groupIds,
                                    childPartition.groupCount,
                                    extensionAttr
                            )
                    );
                }
            }

            currentLevel = nextLevel;
        }

        results.sort(
                Comparator.comparingDouble((ScoredFD fd) -> fd.gpdep).reversed()
                        .thenComparingInt(fd -> fd.lhs.cardinality())
        );

        return results;
    }

    private ScoredFD scoreFromPartition(BitSet lhs, int rhs, int[] groupIds, int groupCount) {
        if (numRecords == 0) {
            return new ScoredFD(
                    cloneBitSet(lhs),
                    rhs,
                    1.0,
                    1.0,
                    0.0,
                    groupCount,
                    true
            );
        }

        Long2IntOpenHashMap pairCounts = new Long2IntOpenHashMap(numRecords * 2);
        pairCounts.defaultReturnValue(0);

        int[] maxPerGroup = new int[groupCount];

        int[] rhsValues = valuesByAttr[rhs];

        for (int row = 0; row < numRecords; row++) {
            int group = groupIds[row];
            int rhsValue = rhsValues[row];

            long key = pack(group, rhsValue);

            int newCount = pairCounts.get(key) + 1;
            pairCounts.put(key, newCount);

            if (newCount > maxPerGroup[group]) {
                maxPerGroup[group] = newCount;
            }
        }

        int sumMajorities = 0;
        for (int max : maxPerGroup) {
            sumMajorities += max;
        }

        double pdep = (double) sumMajorities / (double) numRecords;

        double rhoX = rhoByAttr[rhs];

        double epdep;
        if (numRecords <= 1) {
            epdep = 1.0;
        } else {
            epdep = rhoX
                    + ((groupCount - 1.0) / (numRecords - 1.0))
                    * (1.0 - rhoX);
        }

        double gpdep = Math.max(pdep - epdep, 0.0);
        boolean exact = sumMajorities == numRecords;

        return new ScoredFD(
                cloneBitSet(lhs),
                rhs,
                pdep,
                epdep,
                gpdep,
                groupCount,
                exact
        );
    }

    private Partition refinePartition(int[] parentGroups, int parentGroupCount, int attr) {
        int[] attrValues = valuesByAttr[attr];

        int[] childGroups = new int[numRecords];

        Long2IntOpenHashMap groupMap = new Long2IntOpenHashMap(numRecords * 2);
        groupMap.defaultReturnValue(-1);

        int nextGroupId = 0;

        for (int row = 0; row < numRecords; row++) {
            long key = pack(parentGroups[row], attrValues[row]);

            int groupId = groupMap.get(key);

            if (groupId == -1) {
                groupId = nextGroupId++;
                groupMap.put(key, groupId);
            }

            childGroups[row] = groupId;
        }

        return new Partition(childGroups, nextGroupId);
    }

    private int[][] precomputeValueIds() {
        int[][] values = new int[numAttributes][numRecords];

        for (int attr = 0; attr < numAttributes; attr++) {
            for (int row = 0; row < numRecords; row++) {
                int clusterId = compressedRecords[row][attr];

                if (clusterId >= 0) {
                    values[attr][row] = clusterId;
                } else {
                    values[attr][row] = Integer.MIN_VALUE + row;
                }
            }
        }

        return values;
    }

    private double[] precomputeRhos() {
        double[] rhos = new double[numAttributes];

        for (int attr = 0; attr < numAttributes; attr++) {
            rhos[attr] = computeRhsConcentration(attr);
        }

        return rhos;
    }

    private double computeRhsConcentration(int attr) {
        if (numRecords == 0) {
            return 1.0;
        }

        Long2IntOpenHashMap counts = new Long2IntOpenHashMap(numRecords * 2);
        counts.defaultReturnValue(0);

        int[] values = valuesByAttr[attr];

        for (int row = 0; row < numRecords; row++) {
            long key = values[row];
            counts.put(key, counts.get(key) + 1);
        }

        double sum = 0.0;

        for (int count : counts.values()) {
            sum += (double) count * (double) count;
        }

        return sum / ((double) numRecords * (double) numRecords);
    }

    private static long pack(int first, int second) {
        return (((long) first) << 32) ^ (second & 0xffffffffL);
    }

    private void addIfNonDominated(List<ScoredFD> results, ScoredFD candidate) {
        for (ScoredFD existing : results) {
            if (isSubset(existing.lhs, candidate.lhs)
                    && existing.gpdep + EPS >= candidate.gpdep) {
                return;
            }
        }

        Iterator<ScoredFD> iterator = results.iterator();

        while (iterator.hasNext()) {
            ScoredFD existing = iterator.next();

            if (isSubset(candidate.lhs, existing.lhs)
                    && candidate.gpdep + EPS >= existing.gpdep) {
                iterator.remove();
            }
        }

        results.add(candidate);
    }

    private void addExactMinimal(List<BitSet> exactMinimalLhss, BitSet lhs) {
        for (BitSet existing : exactMinimalLhss) {
            if (isSubset(existing, lhs)) {
                return;
            }
        }

        Iterator<BitSet> iterator = exactMinimalLhss.iterator();

        while (iterator.hasNext()) {
            BitSet existing = iterator.next();

            if (isSubset(lhs, existing)) {
                iterator.remove();
            }
        }

        exactMinimalLhss.add(cloneBitSet(lhs));
    }

    private boolean containsProperSubset(BitSet lhs, List<BitSet> candidates) {
        for (BitSet candidate : candidates) {
            if (candidate.cardinality() < lhs.cardinality()
                    && isSubset(candidate, lhs)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isSubset(BitSet smaller, BitSet larger) {
        BitSet clone = (BitSet) smaller.clone();
        clone.andNot(larger);
        return clone.isEmpty();
    }

    private static BitSet cloneBitSet(BitSet bitSet) {
        return (BitSet) bitSet.clone();
    }

    public static void normalizeGpdepInPlace(List<ScoredFD> fds) {
        if (fds == null || fds.isEmpty()) {
            return;
        }

        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;

        for (ScoredFD fd : fds) {
            min = Math.min(min, fd.gpdep);
            max = Math.max(max, fd.gpdep);
        }

        double range = max - min;

        if (Math.abs(range) < EPS) {
            for (ScoredFD fd : fds) {
                fd.gpdep = 0.0;
            }
            return;
        }

        for (ScoredFD fd : fds) {
            fd.gpdep = (fd.gpdep - min) / range;
        }
    }

    private static class SearchNode {
        final BitSet lhs;
        final int[] groupIds;
        final int groupCount;
        final int lastAddedAttr;

        SearchNode(BitSet lhs, int[] groupIds, int groupCount, int lastAddedAttr) {
            this.lhs = lhs;
            this.groupIds = groupIds;
            this.groupCount = groupCount;
            this.lastAddedAttr = lastAddedAttr;
        }
    }

    private static class Partition {
        final int[] groupIds;
        final int groupCount;

        Partition(int[] groupIds, int groupCount) {
            this.groupIds = groupIds;
            this.groupCount = groupCount;
        }
    }

    public class ScoredFD {
        public final BitSet lhs;
        public final int rhs;

        public final double pdep;
        public final double epdep;
        public double gpdep;

        public final int lhsCardinality;
        public final boolean exact;

        public ScoredFD(
                BitSet lhs,
                int rhs,
                double pdep,
                double epdep,
                double gpdep,
                int lhsCardinality,
                boolean exact
        ) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.pdep = pdep;
            this.epdep = epdep;
            this.gpdep = gpdep;
            this.lhsCardinality = lhsCardinality;
            this.exact = exact;
        }

        public RelaxedFunctionalDependency toRelaxedFunctionalDependency() {
            Set<ColumnIdentifier> determinant = new HashSet<>();

            for (int attr = lhs.nextSetBit(0); attr >= 0; attr = lhs.nextSetBit(attr + 1)) {
                int originalAttr = plis.get(attr).getAttribute();
                determinant.add(columnIdentifiers.get(originalAttr));
            }

            ColumnCombination lhsCombination = new ColumnCombination();
            lhsCombination.setColumnIdentifiers(determinant);

            int originalRhs = plis.get(rhs).getAttribute();
            ColumnIdentifier dependant = columnIdentifiers.get(originalRhs);

            return new RelaxedFunctionalDependency(lhsCombination, dependant, gpdep);
        }

        @Override
        public String toString() {
            return lhsToString(lhs) + " -> " + rhsToString(rhs)
                    + " | pdep=" + pdep
                    + " | epdep=" + epdep
                    + " | gpdep=" + gpdep
                    + " | lhsCardinality=" + lhsCardinality
                    + " | exact=" + exact;
        }
    }

    private String lhsToString(BitSet lhs) {
        List<String> names = new ArrayList<>();

        for (int attr = lhs.nextSetBit(0); attr >= 0; attr = lhs.nextSetBit(attr + 1)) {
            int originalAttr = plis.get(attr).getAttribute();
            names.add(columnIdentifiers.get(originalAttr).toString());
        }

        return names.toString();
    }

    private String rhsToString(int rhs) {
        int originalRhs = plis.get(rhs).getAttribute();
        return columnIdentifiers.get(originalRhs).toString();
    }
}