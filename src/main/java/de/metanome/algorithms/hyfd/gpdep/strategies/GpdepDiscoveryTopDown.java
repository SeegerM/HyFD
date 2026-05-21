package de.metanome.algorithms.hyfd.gpdep.strategies;


import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;
import de.metanome.algorithms.hyfd.gpdep.util.*;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GpdepDiscoveryTopDown extends AbstractGpdepDiscovery {

    public GpdepDiscoveryTopDown(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            List<? extends GpdepExactFD> exactFds
    ) {
        this(
                numAttributes,
                numRecords,
                compressedRecords,
                plis,
                columnIdentifiers,
                maxLhsSize,
                minGpdep,
                0.0d,
                exactFds
        );
    }

    public GpdepDiscoveryTopDown(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial,
            List<? extends GpdepExactFD> exactFds
    ) {
        this(
                numAttributes,
                numRecords,
                compressedRecords,
                plis,
                columnIdentifiers,
                maxLhsSize,
                minGpdep,
                minPartial,
                exactFds,
                ResultMode.ALL_NON_DOMINATED,
                PartitionCacheMode.PER_RHS
        );
    }

    public GpdepDiscoveryTopDown(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            List<? extends GpdepExactFD> exactFds,
            ResultMode resultMode,
            PartitionCacheMode cacheMode
    ) {
        this(
                numAttributes,
                numRecords,
                compressedRecords,
                plis,
                columnIdentifiers,
                maxLhsSize,
                minGpdep,
                0.0d,
                exactFds,
                resultMode,
                cacheMode
        );
    }

    public GpdepDiscoveryTopDown(
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
        super(
                numAttributes,
                numRecords,
                compressedRecords,
                plis,
                columnIdentifiers,
                maxLhsSize,
                minGpdep,
                minPartial,
                exactFds,
                resultMode,
                cacheMode
        );
    }

    @Override
    protected List<ScoredFD> discoverForRhs(int rhs) {
        if (!canMeetGlobalMinGpdep(rhs)) {
            return new ArrayList<>();
        }

        ResultCollector collector = createCollector(rhs);
        Map<BitSet, Partition> localPartitionCache = createLocalPartitionCache();

        Set<BitSet> visited = new HashSet<>();

        for (BitSet exactLhs : collector.exactMinimalLhss()) {
            if (exactLhs.get(rhs)) {
                continue;
            }

            if (exactLhs.cardinality() > maxLhsSize) {
                continue;
            }

            Partition exactPartition = buildPartition(
                    exactLhs,
                    localPartitionCache
            );

            ScoredFD scoredExact = scoreFromPartition(
                    exactLhs,
                    rhs,
                    exactPartition.groupIds,
                    exactPartition.groupCount
            );

            collector.consider(scoredExact);
            visited.add(cloneBitSet(exactLhs));
        }

        Deque<TopDownNode> stack = new ArrayDeque<>();

        for (BitSet topLhs : buildTopLevelLhss(rhs)) {
            if (visited.contains(topLhs)) {
                continue;
            }

            Partition partition = buildPartition(
                    topLhs,
                    localPartitionCache
            );

            stack.push(
                    new TopDownNode(
                            topLhs,
                            partition.groupIds,
                            partition.groupCount
                    )
            );
        }

        while (!stack.isEmpty()) {
            TopDownNode node = stack.pop();
            BitSet lhs = node.lhs;

            if (lhs.get(rhs)) {
                continue;
            }

            if (!visited.add(cloneBitSet(lhs))) {
                continue;
            }

            BitSet containedExactSubset =
                    findContainedProperSubset(lhs, collector.exactMinimalLhss());

            BitSet removeCandidates;

            if (containedExactSubset != null) {
                removeCandidates = containedExactSubset;
            } else {
                ScoredFD scored = scoreFromPartition(
                        lhs,
                        rhs,
                        node.groupIds,
                        node.groupCount
                );

                collector.consider(scored);

                if (scored.exact) {
                    addExactMinimal(collector.exactMinimalLhss(), lhs);
                }

                if (collector.pruneTopDownSubsets(scored, rhoByAttr[rhs])) {
                    continue;
                }

                removeCandidates = lhs;
            }

            if (lhs.cardinality() == 0) {
                continue;
            }

            for (int attr = removeCandidates.nextSetBit(0);
                 attr >= 0;
                 attr = removeCandidates.nextSetBit(attr + 1)) {

                BitSet childLhs = cloneBitSet(lhs);
                childLhs.clear(attr);

                if (childLhs.get(rhs)) {
                    continue;
                }

                if (childLhs.cardinality() > maxLhsSize) {
                    continue;
                }

                if (visited.contains(childLhs)) {
                    continue;
                }

                Partition childPartition = buildPartition(
                        childLhs,
                        localPartitionCache
                );

                stack.push(
                        new TopDownNode(
                                childLhs,
                                childPartition.groupIds,
                                childPartition.groupCount
                        )
                );
            }
        }

        return collector.results();
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
}


