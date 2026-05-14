package de.metanome.algorithms.hyfd;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GpdepDiscoveryTopDown {

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

    private final Map<Integer, List<BitSet>> exactLhssByRhs;

    private final int numThreads;

    public GpdepDiscoveryTopDown(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            List<ExactFD> exactFds
    ) {
        this.numAttributes = numAttributes;
        this.numRecords = numRecords;
        this.compressedRecords = compressedRecords;
        this.plis = plis;
        this.columnIdentifiers = columnIdentifiers;
        this.maxLhsSize = maxLhsSize < 0
                ? numAttributes - 1
                : Math.min(maxLhsSize, numAttributes - 1);
        this.minGpdep = minGpdep;

        this.numThreads = Math.max(
                1,
                Runtime.getRuntime().availableProcessors() - 1
        );

        this.valuesByAttr = precomputeValueIds();
        this.rhoByAttr = precomputeRhos();

        this.exactLhssByRhs = new HashMap<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            this.exactLhssByRhs.put(rhs, new ArrayList<BitSet>());
        }

        if (exactFds != null) {
            for (ExactFD exactFD : exactFds) {
                if (exactFD == null) {
                    continue;
                }

                if (exactFD.rhs < 0 || exactFD.rhs >= numAttributes) {
                    continue;
                }

                if (exactFD.lhs.get(exactFD.rhs)) {
                    continue;
                }

                if (exactFD.lhs.cardinality() > this.maxLhsSize) {
                    continue;
                }

                addExactMinimal(
                        this.exactLhssByRhs.get(exactFD.rhs),
                        exactFD.lhs
                );
            }
        }
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
                throw new RuntimeException("Gpdep top-down discovery interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Gpdep top-down discovery failed", e);
            }
        }

        executor.shutdown();

        sortResults(allResults);

        return allResults;
    }

    public List<ScoredFD> discover() {
        List<ScoredFD> allResults = new ArrayList<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            allResults.addAll(discoverForRhs(rhs));
        }

        sortResults(allResults);

        return allResults;
    }

    private List<ScoredFD> discoverForRhs(int rhs) {
        if ((1.0d - rhoByAttr[rhs]) + EPS < minGpdep) {
            return Collections.emptyList();
        }

        List<ScoredFD> results = new ArrayList<>();

        List<BitSet> exactMinimalLhss = new ArrayList<>();
        for (BitSet exact : exactLhssByRhs.get(rhs)) {
            exactMinimalLhss.add(cloneBitSet(exact));
        }

        Map<BitSet, Partition> partitionCache = new HashMap<>();
        Set<BitSet> visited = new HashSet<>();

        for (BitSet exactLhs : exactMinimalLhss) {
            if (exactLhs.get(rhs)) {
                continue;
            }

            if (exactLhs.cardinality() > maxLhsSize) {
                continue;
            }

            Partition exactPartition = buildPartition(
                    exactLhs,
                    partitionCache
            );

            ScoredFD scoredExact = scoreFromPartition(
                    exactLhs,
                    rhs,
                    exactPartition.groupIds,
                    exactPartition.groupCount
            );

            if (scoredExact.gpdep + EPS >= minGpdep) {
                addIfNonDominated(results, scoredExact);
            }

            visited.add(cloneBitSet(exactLhs));
        }

        Deque<SearchNode> stack = new ArrayDeque<>();

        for (BitSet topLhs : buildTopLevelLhss(rhs)) {
            Partition partition = buildPartition(topLhs, partitionCache);
            stack.push(new SearchNode(topLhs, partition.groupIds, partition.groupCount));
        }

        while (!stack.isEmpty()) {
            SearchNode node = stack.pop();

            BitSet lhs = node.lhs;

            if (lhs.get(rhs)) {
                continue;
            }

            BitSet visitedKey = cloneBitSet(lhs);
            if (!visited.add(visitedKey)) {
                continue;
            }

            BitSet containedExactSubset =
                    findContainedProperSubset(lhs, exactMinimalLhss);

            if (containedExactSubset == null) {
                ScoredFD scored = scoreFromPartition(
                        lhs,
                        rhs,
                        node.groupIds,
                        node.groupCount
                );

                if (scored.gpdep + EPS >= minGpdep) {
                    addIfNonDominated(results, scored);
                }

                if (scored.exact) {
                    addExactMinimal(exactMinimalLhss, lhs);
                }
            }

            if (lhs.cardinality() == 0) {
                continue;
            }

            BitSet removeCandidates;

            if (containedExactSubset != null) {
                removeCandidates = containedExactSubset;
            } else {
                removeCandidates = lhs;
            }

            for (int attr = removeCandidates.nextSetBit(0);
                 attr >= 0;
                 attr = removeCandidates.nextSetBit(attr + 1)) {

                BitSet childLhs = cloneBitSet(lhs);
                childLhs.clear(attr);

                if (childLhs.cardinality() > maxLhsSize) {
                    continue;
                }

                if (visited.contains(childLhs)) {
                    continue;
                }

                Partition childPartition = buildPartition(
                        childLhs,
                        partitionCache
                );

                stack.push(
                        new SearchNode(
                                childLhs,
                                childPartition.groupIds,
                                childPartition.groupCount
                        )
                );
            }
        }

        sortResults(results);

        return results;
    }

    private List<BitSet> buildTopLevelLhss(int rhs) {
        List<BitSet> topLevel = new ArrayList<>();

        int targetSize = Math.min(maxLhsSize, numAttributes - 1);

        BitSet current = new BitSet(numAttributes);
        buildTopLevelLhssRecursive(rhs, targetSize, 0, current, topLevel);

        return topLevel;
    }

    private void buildTopLevelLhssRecursive(
            int rhs,
            int targetSize,
            int startAttr,
            BitSet current,
            List<BitSet> result
    ) {
        if (current.cardinality() == targetSize) {
            result.add(cloneBitSet(current));
            return;
        }

        for (int attr = startAttr; attr < numAttributes; attr++) {
            if (attr == rhs) {
                continue;
            }

            current.set(attr);
            buildTopLevelLhssRecursive(
                    rhs,
                    targetSize,
                    attr + 1,
                    current,
                    result
            );
            current.clear(attr);
        }
    }

    private Partition buildPartition(
            BitSet lhs,
            Map<BitSet, Partition> partitionCache
    ) {
        BitSet cacheKey = cloneBitSet(lhs);

        Partition cached = partitionCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        int[] groupIds = new int[numRecords];
        Arrays.fill(groupIds, 0);

        int groupCount = 1;

        for (int attr = lhs.nextSetBit(0);
             attr >= 0;
             attr = lhs.nextSetBit(attr + 1)) {

            Partition refined = refinePartition(groupIds, groupCount, attr);
            groupIds = refined.groupIds;
            groupCount = refined.groupCount;
        }

        Partition partition = new Partition(groupIds, groupCount);
        partitionCache.put(cacheKey, partition);

        return partition;
    }

    private ScoredFD scoreFromPartition(
            BitSet lhs,
            int rhs,
            int[] groupIds,
            int groupCount
    ) {
        if (numRecords == 0) {
            return new ScoredFD(
                    cloneBitSet(lhs),
                    rhs,
                    1.0d,
                    1.0d,
                    0.0d,
                    lhs.cardinality(),
                    groupCount,
                    true
            );
        }

        Long2IntOpenHashMap pairCounts =
                new Long2IntOpenHashMap(numRecords * 2);
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

        double rhoY = rhoByAttr[rhs];

        double epdep;

        if (numRecords <= 1) {
            epdep = 1.0d;
        } else {
            epdep = rhoY
                    + ((groupCount - 1.0d) / (numRecords - 1.0d))
                    * (1.0d - rhoY);
        }

        double gpdep = Math.max(pdep - epdep, 0.0d);

        boolean exact = sumMajorities == numRecords;

        return new ScoredFD(
                cloneBitSet(lhs),
                rhs,
                pdep,
                epdep,
                gpdep,
                lhs.cardinality(),
                groupCount,
                exact
        );
    }

    private Partition refinePartition(
            int[] parentGroups,
            int parentGroupCount,
            int attr
    ) {
        int[] attrValues = valuesByAttr[attr];

        int[] childGroups = new int[numRecords];

        Long2IntOpenHashMap groupMap =
                new Long2IntOpenHashMap(numRecords * 2);
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
            return 1.0d;
        }

        Long2IntOpenHashMap counts =
                new Long2IntOpenHashMap(numRecords * 2);
        counts.defaultReturnValue(0);

        int[] values = valuesByAttr[attr];

        for (int row = 0; row < numRecords; row++) {
            long key = values[row];
            counts.put(key, counts.get(key) + 1);
        }

        double sum = 0.0d;

        for (int count : counts.values()) {
            sum += (double) count * (double) count;
        }

        return sum / ((double) numRecords * (double) numRecords);
    }
    private void addIfBestComparable(List<ScoredFD> results, ScoredFD candidate) {

        for (ScoredFD existing : results) {
            if (existing.rhs != candidate.rhs) {
                continue;
            }

            if (!areComparable(existing.lhs, candidate.lhs)) {
                continue;
            }

            if (existing.gpdep > candidate.gpdep + EPS) {
                return;
            }
        }

        Iterator<ScoredFD> iterator = results.iterator();

        while (iterator.hasNext()) {
            ScoredFD existing = iterator.next();

            if (existing.rhs != candidate.rhs) {
                continue;
            }

            if (!areComparable(existing.lhs, candidate.lhs)) {
                continue;
            }

            if (candidate.gpdep > existing.gpdep + EPS) {
                iterator.remove();
            }
        }

        results.add(candidate);
    }

    private static boolean areComparable(BitSet first, BitSet second) {
        return isSubset(first, second) || isSubset(second, first);
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

    private static void addExactMinimal(List<BitSet> exactMinimalLhss, BitSet lhs) {
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

    private BitSet findContainedProperSubset(
            BitSet lhs,
            List<BitSet> candidates
    ) {
        for (BitSet candidate : candidates) {
            if (candidate.cardinality() < lhs.cardinality()
                    && isSubset(candidate, lhs)) {
                return candidate;
            }
        }

        return null;
    }

    private static boolean isSubset(BitSet smaller, BitSet larger) {
        BitSet clone = cloneBitSet(smaller);
        clone.andNot(larger);
        return clone.isEmpty();
    }

    private static BitSet cloneBitSet(BitSet bitSet) {
        return (BitSet) bitSet.clone();
    }

    private static long pack(int first, int second) {
        return (((long) first) << 32) ^ (second & 0xffffffffL);
    }

    private static void sortResults(List<ScoredFD> results) {
        results.sort(
                Comparator.comparingDouble((ScoredFD fd) -> fd.gpdep).reversed()
                        .thenComparingInt(fd -> fd.lhsCardinality)
                        .thenComparingInt(fd -> fd.rhs)
        );
    }

    public static class ExactFD {
        public final BitSet lhs;
        public final int rhs;

        public ExactFD(BitSet lhs, int rhs) {
            this.lhs = cloneBitSet(lhs);
            this.rhs = rhs;
        }
    }

    private static class SearchNode {
        final BitSet lhs;
        final int[] groupIds;
        final int groupCount;

        SearchNode(BitSet lhs, int[] groupIds, int groupCount) {
            this.lhs = cloneBitSet(lhs);
            this.groupIds = groupIds;
            this.groupCount = groupCount;
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
        public final int groupCount;
        public final boolean exact;

        public ScoredFD(
                BitSet lhs,
                int rhs,
                double pdep,
                double epdep,
                double gpdep,
                int lhsCardinality,
                int groupCount,
                boolean exact
        ) {
            this.lhs = cloneBitSet(lhs);
            this.rhs = rhs;
            this.pdep = pdep;
            this.epdep = epdep;
            this.gpdep = gpdep;
            this.lhsCardinality = lhsCardinality;
            this.groupCount = groupCount;
            this.exact = exact;
        }

        public RelaxedFunctionalDependency toRelaxedFunctionalDependency() {
            Set<ColumnIdentifier> determinant = new HashSet<>();

            for (int attr = lhs.nextSetBit(0);
                 attr >= 0;
                 attr = lhs.nextSetBit(attr + 1)) {

                int originalAttr = plis.get(attr).getAttribute();
                determinant.add(columnIdentifiers.get(originalAttr));
            }

            ColumnCombination lhsCombination = new ColumnCombination();
            lhsCombination.setColumnIdentifiers(determinant);

            int originalRhs = plis.get(rhs).getAttribute();
            ColumnIdentifier dependant = columnIdentifiers.get(originalRhs);

            return new RelaxedFunctionalDependency(
                    lhsCombination,
                    dependant,
                    gpdep
            );
        }

        @Override
        public String toString() {
            return lhsToString(lhs) + " -> " + rhsToString(rhs)
                    + " | pdep=" + pdep
                    + " | epdep=" + epdep
                    + " | gpdep=" + gpdep
                    + " | lhsCardinality=" + lhsCardinality
                    + " | groupCount=" + groupCount
                    + " | exact=" + exact;
        }
    }

    private String lhsToString(BitSet lhs) {
        List<String> names = new ArrayList<>();

        for (int attr = lhs.nextSetBit(0);
             attr >= 0;
             attr = lhs.nextSetBit(attr + 1)) {

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