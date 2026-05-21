package de.metanome.algorithms.hyfd.gpdep.strategies;


import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.gpdep.util.PartitionCacheMode;
import de.metanome.algorithms.hyfd.gpdep.util.ResultMode;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

public class GpdepDiscoveryBottomUpCache extends GpdepDiscoveryBottomUp {

    public GpdepDiscoveryBottomUpCache(
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

    public GpdepDiscoveryBottomUpCache(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial
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
                null,
                ResultMode.ALL_NON_DOMINATED,
                PartitionCacheMode.GLOBAL_CONCURRENT
        );
    }
}
