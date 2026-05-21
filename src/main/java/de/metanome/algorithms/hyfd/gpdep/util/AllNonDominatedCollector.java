package de.metanome.algorithms.hyfd.gpdep.util;


import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public final class AllNonDominatedCollector implements ResultCollector {

    private final double minGpdep;
    private final double minPartial;
    private final List<BitSet> exactMinimalLhss;
    private final List<ScoredFD> results = new ArrayList<>();

    public AllNonDominatedCollector(
            double minGpdep,
            double minPartial,
            List<BitSet> exactMinimalLhss
    ) {
        this.minGpdep = minGpdep;
        this.minPartial = minPartial;
        this.exactMinimalLhss = exactMinimalLhss;
    }

    @Override
    public void consider(ScoredFD candidate) {
        if (!meetsThresholds(candidate)) {
            return;
        }

        addIfNonDominated(candidate);
    }

    @Override
    public List<ScoredFD> results() {
        AbstractGpdepDiscovery.sortResults(results);
        return results;
    }

    @Override
    public List<BitSet> exactMinimalLhss() {
        return exactMinimalLhss;
    }

    @Override
    public boolean pruneBottomUpDescendants(double upperBoundForDescendants) {
        /*
         * Bottom-up descendants may increase pdep/partial, so minPartial alone
         * must not prune here. The gpdep upper bound is still safe.
         */
        return upperBoundForDescendants + AbstractGpdepDiscovery.EPS < minGpdep;
    }

    @Override
    public boolean pruneTopDownSubsets(ScoredFD scored, double rhsRho) {
        /*
         * Top-down subsets cannot have higher pdep than the current LHS.
         * If the current LHS does not meet minPartial, no subset can meet it.
         */
        return scored.pdep + AbstractGpdepDiscovery.EPS < minPartial;
    }

    private boolean meetsThresholds(ScoredFD candidate) {
        return candidate.gpdep + AbstractGpdepDiscovery.EPS >= minGpdep
                && candidate.pdep + AbstractGpdepDiscovery.EPS >= minPartial;
    }

    private void addIfNonDominated(ScoredFD candidate) {
        for (ScoredFD existing : results) {
            if (AbstractGpdepDiscovery.isSubset(existing.lhs, candidate.lhs)
                    && existing.gpdep + AbstractGpdepDiscovery.EPS >= candidate.gpdep
                    && existing.pdep + AbstractGpdepDiscovery.EPS >= candidate.pdep) {
                return;
            }
        }

        Iterator<ScoredFD> iterator = results.iterator();

        while (iterator.hasNext()) {
            ScoredFD existing = iterator.next();

            if (AbstractGpdepDiscovery.isSubset(candidate.lhs, existing.lhs)
                    && candidate.gpdep + AbstractGpdepDiscovery.EPS >= existing.gpdep
                    && candidate.pdep + AbstractGpdepDiscovery.EPS >= existing.pdep) {
                iterator.remove();
            }
        }

        results.add(candidate);
    }
}


