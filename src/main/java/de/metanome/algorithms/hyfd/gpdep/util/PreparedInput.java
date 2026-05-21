package de.metanome.algorithms.hyfd.gpdep.util;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

public class PreparedInput {
    public final List<PositionListIndex> plis;
    public final int numRecords;
    public final int[][] compressedRecords;
    public final ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    public final int effectiveMaxLhsSize;

    public PreparedInput(
            List<PositionListIndex> plis,
            int numRecords,
            int[][] compressedRecords,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers,
            int effectiveMaxLhsSize
    ) {
        this.plis = plis;
        this.numRecords = numRecords;
        this.compressedRecords = compressedRecords;
        this.columnIdentifiers = columnIdentifiers;
        this.effectiveMaxLhsSize = effectiveMaxLhsSize;
    }
}
