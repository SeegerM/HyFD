package de.metanome.algorithms.hyfd.structures;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;

public class WeightedPartialFunctionalDependency extends RelaxedFunctionalDependency {

    double weight;
    public WeightedPartialFunctionalDependency() {
        super();
        weight = 1d;
    }

    public WeightedPartialFunctionalDependency(ColumnCombination determinant,
                                       ColumnIdentifier dependant,
                                       Double measure, Double weight) {
        super(determinant, dependant, measure);
        this.weight = weight;
    }

    @Override
    public String toString() {
        return determinant.toString() + FD_SEPARATOR + dependant.toString() + TABLEAU_SEPARATOR + measure + TABLEAU_SEPARATOR + weight;
    }
}
