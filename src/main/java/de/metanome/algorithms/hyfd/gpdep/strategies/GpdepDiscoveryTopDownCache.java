package de.metanome.algorithms.hyfd.gpdep.strategies;


import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.gpdep.util.GpdepExactFD;
import de.metanome.algorithms.hyfd.gpdep.util.PartitionCacheMode;
import de.metanome.algorithms.hyfd.gpdep.util.ResultMode;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.BitSet;
import java.util.List;


public class GpdepDiscoveryTopDownCache extends GpdepDiscoveryTopDown {

    public GpdepDiscoveryTopDownCache(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            List<ExactFD> exactFds
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

    public GpdepDiscoveryTopDownCache(
            int numAttributes,
            int numRecords,
            int[][] compressedRecords,
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int maxLhsSize,
            double minGpdep,
            double minPartial,
            List<ExactFD> exactFds
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
                ResultMode.ALL_NON_DOMINATED,
                PartitionCacheMode.GLOBAL_CONCURRENT
        );
    }

    public static class ExactFD extends GpdepExactFD {
        public ExactFD(BitSet lhs, int rhs) {
            super(lhs, rhs);
        }
    }
}

