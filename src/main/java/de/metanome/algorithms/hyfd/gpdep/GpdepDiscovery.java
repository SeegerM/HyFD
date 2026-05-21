package de.metanome.algorithms.hyfd.gpdep;

import de.metanome.algorithms.hyfd.gpdep.util.ScoredFD;

import java.util.List;

public interface GpdepDiscovery {

    List<ScoredFD> discover();

    List<ScoredFD> discoverParallel();
}
