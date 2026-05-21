package de.metanome.algorithms.hyfd.gpdep.util;


import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public final class BestPerRhsCollector implements ResultCollector {

    private final double minGpdep;
    private final double minPartial;
    private final List<BitSet> exactMinimalLhss;

    private ScoredFD best;

    public BestPerRhsCollector(
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

        if (isBetter(candidate, best)) {
            best = candidate;
        }
    }

    @Override
    public List<ScoredFD> results() {
        if (best == null) {
            return Collections.emptyList();
        }

        return Collections.singletonList(best);
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
        if (best != null) {
            /*
             * Descendants have larger LHSs, so equality is not enough to win the
             * tie-breaker that prefers smaller LHSs.
             */
            return upperBoundForDescendants <= best.gpdep + AbstractGpdepDiscovery.EPS;
        }

        return upperBoundForDescendants + AbstractGpdepDiscovery.EPS < minGpdep;
    }

    @Override
    public boolean pruneTopDownSubsets(ScoredFD scored, double rhsRho) {
        /*
         * Top-down subsets cannot have higher pdep than the current LHS.
         * If the current LHS does not meet minPartial, no subset can meet it.
         */
        if (scored.pdep + AbstractGpdepDiscovery.EPS < minPartial) {
            return true;
        }

        /*
         * Top-down gpdep bound:
         *   pdep(T, rhs) <= pdep(lhs, rhs)
         *   epdep(T, rhs) >= rho(rhs)
         * Therefore:
         *   gpdep(T, rhs) <= pdep(lhs, rhs) - rho(rhs)
         */
        double subsetUpperBound = Math.max(scored.pdep - rhsRho, 0.0d);

        double currentBest = best == null
                ? minGpdep
                : Math.max(minGpdep, best.gpdep);

        /*
         * Strict pruning only. If the upper bound ties the current best, a
         * smaller subset may still win by the tie-breaker.
         */
        return subsetUpperBound + AbstractGpdepDiscovery.EPS < currentBest;
    }

    private boolean meetsThresholds(ScoredFD candidate) {
        return candidate.gpdep + AbstractGpdepDiscovery.EPS >= minGpdep
                && candidate.pdep + AbstractGpdepDiscovery.EPS >= minPartial;
    }

    private boolean isBetter(ScoredFD candidate, ScoredFD incumbent) {
        if (incumbent == null) {
            return true;
        }

        if (candidate.gpdep > incumbent.gpdep + AbstractGpdepDiscovery.EPS) {
            return true;
        }

        if (candidate.gpdep + AbstractGpdepDiscovery.EPS < incumbent.gpdep) {
            return false;
        }

        if (candidate.lhsCardinality < incumbent.lhsCardinality) {
            return true;
        }

        if (candidate.lhsCardinality > incumbent.lhsCardinality) {
            return false;
        }

        if (candidate.pdep > incumbent.pdep + AbstractGpdepDiscovery.EPS) {
            return true;
        }

        if (candidate.pdep + AbstractGpdepDiscovery.EPS < incumbent.pdep) {
            return false;
        }

        if (candidate.groupCount < incumbent.groupCount) {
            return true;
        }

        if (candidate.groupCount > incumbent.groupCount) {
            return false;
        }

        return AbstractGpdepDiscovery.compareBitSets(candidate.lhs, incumbent.lhs) < 0;
    }
}

