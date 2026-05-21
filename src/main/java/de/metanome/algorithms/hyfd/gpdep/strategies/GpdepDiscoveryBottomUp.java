package de.metanome.algorithms.hyfd.gpdep.strategies;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;
import de.metanome.algorithms.hyfd.gpdep.util.*;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class GpdepDiscoveryBottomUp extends AbstractGpdepDiscovery {

    public GpdepDiscoveryBottomUp(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep
    ) {
        this(
                numAttributes,
                numRecords,
                compressedRecords,
                plis,
                columnIdentifiers,
                maxLhsSize,
                minGpdep,
                0.0d
        );
    }

    public GpdepDiscoveryBottomUp(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial
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
                null,
                ResultMode.ALL_NON_DOMINATED,
                PartitionCacheMode.NONE
        );
    }

    public GpdepDiscoveryBottomUp(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            boolean usePartitionCache
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
                null,
                ResultMode.ALL_NON_DOMINATED,
                usePartitionCache
                        ? PartitionCacheMode.GLOBAL_CONCURRENT
                        : PartitionCacheMode.NONE
        );
    }

    public GpdepDiscoveryBottomUp(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial,
            boolean usePartitionCache
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
                null,
                ResultMode.ALL_NON_DOMINATED,
                usePartitionCache
                        ? PartitionCacheMode.GLOBAL_CONCURRENT
                        : PartitionCacheMode.NONE
        );
    }

    public GpdepDiscoveryBottomUp(
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

    public GpdepDiscoveryBottomUp(
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

        Partition rootPartition = rootPartition(localPartitionCache);

        List<BottomUpNode> currentLevel = new ArrayList<>();
        currentLevel.add(
                new BottomUpNode(
                        new BitSet(numAttributes),
                        rootPartition.groupIds,
                        rootPartition.groupCount,
                        -1
                )
        );

        while (!currentLevel.isEmpty()) {
            List<BottomUpNode> nextLevel = new ArrayList<>();

            for (BottomUpNode node : currentLevel) {
                BitSet lhs = node.lhs;

                if (lhs.get(rhs)) {
                    continue;
                }

                if (containsProperSubset(lhs, collector.exactMinimalLhss())) {
                    continue;
                }

                ScoredFD scored = scoreFromPartition(
                        lhs,
                        rhs,
                        node.groupIds,
                        node.groupCount
                );

                collector.consider(scored);

                if (scored.exact) {
                    addExactMinimal(collector.exactMinimalLhss(), lhs);
                    continue;
                }

                if (lhs.cardinality() >= maxLhsSize) {
                    continue;
                }

                double upperBoundForDescendants =
                        Math.max(1.0d - scored.epdep, 0.0d);

                if (collector.pruneBottomUpDescendants(upperBoundForDescendants)) {
                    continue;
                }

                for (int extensionAttr = node.lastAddedAttr + 1;
                     extensionAttr < numAttributes;
                     extensionAttr++) {

                    if (extensionAttr == rhs) {
                        continue;
                    }

                    BitSet childLhs = cloneBitSet(lhs);
                    childLhs.set(extensionAttr);

                    if (containsProperSubset(childLhs, collector.exactMinimalLhss())) {
                        continue;
                    }

                    Partition childPartition = childPartition(
                            childLhs,
                            node.groupIds,
                            node.groupCount,
                            extensionAttr,
                            localPartitionCache
                    );

                    nextLevel.add(
                            new BottomUpNode(
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

        return collector.results();
    }
}

