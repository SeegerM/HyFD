package de.metanome.algorithms.hyfd.gpdep;


import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithms.hyfd.gpdep.util.*;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public abstract class AbstractGpdepDiscovery implements GpdepDiscovery {

    public static final double EPS = 1e-12;

    protected final int numAttributes;
    protected final int numRecords;
    protected final int[][] compressedRecords;
    protected final List<PositionListIndex> plis;
    protected final ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    protected final int maxLhsSize;
    protected final double minGpdep;
    protected final double minPartial;

    protected final int[][] valuesByAttr;
    protected final double[] rhoByAttr;

    protected final int numThreads;

    protected final ResultMode resultMode;
    protected final PartitionCacheMode cacheMode;

    private final ConcurrentMap<BitSet, Partition> globalPartitionCache;
    private final Map<Integer, List<BitSet>> exactLhssByRhs;

    protected AbstractGpdepDiscovery(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial,
            List<? extends GpdepExactFD> exactFds,
            ResultMode resultMode,
            PartitionCacheMode cacheMode
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
        this.minPartial = minPartial;
        this.resultMode = resultMode == null
                ? ResultMode.ALL_NON_DOMINATED
                : resultMode;
        this.cacheMode = cacheMode == null
                ? PartitionCacheMode.NONE
                : cacheMode;

        this.numThreads = Math.max(
                1,
                Runtime.getRuntime().availableProcessors() - 1
        );

        this.valuesByAttr = precomputeValueIds();
        this.rhoByAttr = precomputeRhos();

        this.globalPartitionCache = this.cacheMode == PartitionCacheMode.GLOBAL_CONCURRENT
                ? new ConcurrentHashMap<>()
                : null;

        this.exactLhssByRhs = initializeExactLhssByRhs(exactFds);
    }

    @Override
    public List<ScoredFD> discover() {
        List<ScoredFD> allResults = new ArrayList<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            allResults.addAll(discoverForRhs(rhs));
        }

        sortResults(allResults);
        return allResults;
    }

    @Override
    public List<ScoredFD> discoverParallel() {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<List<ScoredFD>>> futures = new ArrayList<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            final int rhsAttr = rhs;
            futures.add(executor.submit(() -> discoverForRhs(rhsAttr)));
        }

        List<ScoredFD> allResults = new ArrayList<>();

        try {
            for (Future<List<ScoredFD>> future : futures) {
                allResults.addAll(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("GPDEP discovery interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("GPDEP discovery failed", e);
        } finally {
            executor.shutdownNow();
        }

        sortResults(allResults);
        return allResults;
    }

    protected abstract List<ScoredFD> discoverForRhs(int rhs);

    protected ResultCollector createCollector(int rhs) {
        List<BitSet> exactMinimalLhss = copyExactMinimalLhss(rhs);

        if (resultMode == ResultMode.BEST_PER_RHS) {
            return new BestPerRhsCollector(minGpdep, minPartial, exactMinimalLhss);
        }

        return new AllNonDominatedCollector(minGpdep, minPartial, exactMinimalLhss);
    }

    protected boolean canMeetGlobalMinGpdep(int rhs) {
        return Math.max(1.0d - rhoByAttr[rhs], 0.0d) + EPS >= minGpdep;
    }

    protected Map<BitSet, Partition> createLocalPartitionCache() {
        if (cacheMode == PartitionCacheMode.PER_RHS) {
            return new HashMap<>();
        }

        return null;
    }

    protected Partition rootPartition(Map<BitSet, Partition> localCache) {
        BitSet emptyLhs = new BitSet(numAttributes);

        return cachedOrCompute(emptyLhs, localCache, () -> {
            int[] rootGroups = new int[numRecords];
            Arrays.fill(rootGroups, 0);
            return new Partition(rootGroups, 1);
        });
    }

    protected Partition childPartition(
            BitSet lhs,
            int[] parentGroups,
            int parentGroupCount,
            int extensionAttr,
            Map<BitSet, Partition> localCache
    ) {
        return cachedOrCompute(
                lhs,
                localCache,
                () -> refinePartition(parentGroups, parentGroupCount, extensionAttr)
        );
    }

    protected Partition buildPartition(
            BitSet lhs,
            Map<BitSet, Partition> localCache
    ) {
        return cachedOrCompute(lhs, localCache, () -> {
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

            return new Partition(groupIds, groupCount);
        });
    }

    private Partition cachedOrCompute(
            BitSet lhs,
            Map<BitSet, Partition> localCache,
            Supplier<Partition> supplier
    ) {
        if (cacheMode == PartitionCacheMode.NONE) {
            return supplier.get();
        }

        BitSet cacheKey = cloneBitSet(lhs);

        if (cacheMode == PartitionCacheMode.GLOBAL_CONCURRENT) {
            return globalPartitionCache.computeIfAbsent(cacheKey, ignored -> supplier.get());
        }

        Partition cached = localCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        Partition computed = supplier.get();
        localCache.put(cacheKey, computed);

        return computed;
    }

    protected ScoredFD scoreFromPartition(
            BitSet lhs,
            int rhs,
            int[] groupIds,
            int groupCount
    ) {
        if (numRecords == 0) {
            return new ScoredFD(
                    lhs,
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

        double epdep = numRecords <= 1
                ? 1.0d
                : rhoY
                + ((groupCount - 1.0d) / (numRecords - 1.0d))
                * (1.0d - rhoY);

        double gpdep = Math.max(pdep - epdep, 0.0d);
        boolean exact = sumMajorities == numRecords;

        return new ScoredFD(
                lhs,
                rhs,
                pdep,
                epdep,
                gpdep,
                lhs.cardinality(),
                groupCount,
                exact
        );
    }

    protected Partition refinePartition(
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

                values[attr][row] = clusterId >= 0
                        ? clusterId
                        : Integer.MIN_VALUE + row;
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

    private Map<Integer, List<BitSet>> initializeExactLhssByRhs(
            List<? extends GpdepExactFD> exactFds
    ) {
        Map<Integer, List<BitSet>> exacts = new HashMap<>();

        for (int rhs = 0; rhs < numAttributes; rhs++) {
            exacts.put(rhs, new ArrayList<>());
        }

        if (exactFds == null) {
            return exacts;
        }

        for (GpdepExactFD exactFD : exactFds) {
            if (exactFD == null) {
                continue;
            }

            if (exactFD.rhs < 0 || exactFD.rhs >= numAttributes) {
                continue;
            }

            if (exactFD.lhs.get(exactFD.rhs)) {
                continue;
            }

            if (exactFD.lhs.cardinality() > maxLhsSize) {
                continue;
            }

            addExactMinimal(exacts.get(exactFD.rhs), exactFD.lhs);
        }

        return exacts;
    }

    private List<BitSet> copyExactMinimalLhss(int rhs) {
        List<BitSet> copy = new ArrayList<>();

        List<BitSet> source = exactLhssByRhs.get(rhs);
        if (source == null) {
            return copy;
        }

        for (BitSet exact : source) {
            copy.add(cloneBitSet(exact));
        }

        return copy;
    }

    protected static void addExactMinimal(
            List<BitSet> exactMinimalLhss,
            BitSet lhs
    ) {
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

    protected static boolean containsProperSubset(
            BitSet lhs,
            List<BitSet> candidates
    ) {
        return findContainedProperSubset(lhs, candidates) != null;
    }

    protected static BitSet findContainedProperSubset(
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

    public static boolean isSubset(BitSet smaller, BitSet larger) {
        BitSet clone = cloneBitSet(smaller);
        clone.andNot(larger);
        return clone.isEmpty();
    }

    public static BitSet cloneBitSet(BitSet bitSet) {
        return (BitSet) bitSet.clone();
    }

    protected static long pack(int first, int second) {
        return (((long) first) << 32) ^ (second & 0xffffffffL);
    }

    public static int compareBitSets(BitSet first, BitSet second) {
        int maxLength = Math.max(first.length(), second.length());

        for (int i = 0; i < maxLength; i++) {
            boolean a = first.get(i);
            boolean b = second.get(i);

            if (a != b) {
                return a ? -1 : 1;
            }
        }

        return 0;
    }

    public static void sortResults(List<ScoredFD> results) {
        results.sort(
                Comparator.comparingDouble((ScoredFD fd) -> fd.gpdep).reversed()
                        .thenComparingInt(fd -> fd.lhsCardinality)
                        .thenComparingInt(fd -> fd.rhs)
        );
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
                fd.gpdep = 0.0d;
            }
            return;
        }

        for (ScoredFD fd : fds) {
            fd.gpdep = (fd.gpdep - min) / range;
        }
    }

    public RelaxedFunctionalDependency toRelaxedFunctionalDependency(ScoredFD fd) {
        return fd.toRelaxedFunctionalDependency(plis, columnIdentifiers);
    }

    public List<RelaxedFunctionalDependency> toRelaxedFunctionalDependencies(
            List<ScoredFD> fds
    ) {
        if (fds == null || fds.isEmpty()) {
            return Collections.emptyList();
        }

        List<RelaxedFunctionalDependency> result = new ArrayList<>();

        for (ScoredFD fd : fds) {
            result.add(toRelaxedFunctionalDependency(fd));
        }

        return result;
    }

    public String format(ScoredFD fd) {
        return lhsToString(fd.lhs) + " -> " + rhsToString(fd.rhs)
                + " | pdep=" + fd.pdep
                + " | epdep=" + fd.epdep
                + " | gpdep=" + fd.gpdep
                + " | lhsCardinality=" + fd.lhsCardinality
                + " | groupCount=" + fd.groupCount
                + " | exact=" + fd.exact;
    }

    protected String lhsToString(BitSet lhs) {
        List<String> names = new ArrayList<>();

        for (int attr = lhs.nextSetBit(0);
             attr >= 0;
             attr = lhs.nextSetBit(attr + 1)) {

            int originalAttr = plis.get(attr).getAttribute();
            names.add(columnIdentifiers.get(originalAttr).toString());
        }

        return names.toString();
    }

    protected String rhsToString(int rhs) {
        int originalRhs = plis.get(rhs).getAttribute();
        return columnIdentifiers.get(originalRhs).toString();
    }

    protected static class BottomUpNode {
        public final BitSet lhs;
        public final int[] groupIds;
        public final int groupCount;
        public final int lastAddedAttr;

        public BottomUpNode(
                BitSet lhs,
                int[] groupIds,
                int groupCount,
                int lastAddedAttr
        ) {
            this.lhs = cloneBitSet(lhs);
            this.groupIds = groupIds;
            this.groupCount = groupCount;
            this.lastAddedAttr = lastAddedAttr;
        }
    }

    protected static class TopDownNode {
        public final BitSet lhs;
        public final int[] groupIds;
        public final int groupCount;

        public TopDownNode(BitSet lhs, int[] groupIds, int groupCount) {
            this.lhs = cloneBitSet(lhs);
            this.groupIds = groupIds;
            this.groupCount = groupCount;
        }
    }

    protected static class Partition {
        public final int[] groupIds;
        public final int groupCount;

        Partition(int[] groupIds, int groupCount) {
            this.groupIds = groupIds;
            this.groupCount = groupCount;
        }
    }
}


