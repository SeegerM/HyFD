package de.metanome.algorithms.hyfd.gpdep.util;

import java.util.BitSet;
import java.util.List;

public interface ResultCollector {

    void consider(ScoredFD candidate);

    List<ScoredFD> results();

    List<BitSet> exactMinimalLhss();

    boolean pruneBottomUpDescendants(double upperBoundForDescendants);

    boolean pruneTopDownSubsets(ScoredFD scored, double rhsRho);
}

