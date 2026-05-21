package de.metanome.algorithms.hyfd.gpdep.util;


import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import de.metanome.algorithms.hyfd.structures.WeightedPartialFunctionalDependency;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        this.lhs = AbstractGpdepDiscovery.cloneBitSet(lhs);
        this.rhs = rhs;
        this.pdep = pdep;
        this.epdep = epdep;
        this.gpdep = gpdep;
        this.lhsCardinality = lhsCardinality;
        this.groupCount = groupCount;
        this.exact = exact;
    }

    public RelaxedFunctionalDependency toRelaxedFunctionalDependency(
            List<PositionListIndex> plis,
            ObjectArrayList<ColumnIdentifier> columnIdentifiers
    ) {
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

        return new WeightedPartialFunctionalDependency(lhsCombination, dependant, pdep, gpdep);
    }

    @Override
    public String toString() {
        return lhs + " -> " + rhs
                + " | pdep=" + pdep
                + " | epdep=" + epdep
                + " | gpdep=" + gpdep
                + " | lhsCardinality=" + lhsCardinality
                + " | groupCount=" + groupCount
                + " | exact=" + exact;
    }
}

