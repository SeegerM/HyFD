package de.metanome.algorithms.hyfd.utils.multitable;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;

import java.util.*;
import java.util.stream.Collectors;

public final class FDKey {
    public final List<ColumnIdentifier> determinant; // canonical order
    public final ColumnIdentifier dependant;

    public FDKey(List<ColumnIdentifier> determinant, ColumnIdentifier dependant) {
        // Canonicalize determinant order for consistent equals/hashCode
        this.determinant = new ArrayList<>(determinant);
        this.determinant.sort(Comparator
                .comparing(ColumnIdentifier::getTableIdentifier)
                .thenComparing(ColumnIdentifier::getColumnIdentifier));
        this.dependant = dependant;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FDKey)) return false;
        FDKey that = (FDKey) o;
        return determinant.equals(that.determinant) && dependant.equals(that.dependant);
    }

    @Override public int hashCode() {
        return Objects.hash(determinant, dependant);
    }

    @Override public String toString() {
        String lhs = determinant.stream()
                //.map(ci -> ci.getTableIdentifier() + "." + ci.getColumnIdentifier())
                .map(ColumnIdentifier::getColumnIdentifier)
                .collect(Collectors.joining(","));
        //String rhs = dependant.getTableIdentifier() + "." + dependant.getColumnIdentifier();
        String rhs = dependant.getColumnIdentifier();
        return lhs + " -> " + rhs;
    }

    public static Set<FDKey> extractFDs(List<Result> hyfdResults) {
        Set<FDKey> out = new HashSet<>();
        for (Result r : hyfdResults) {
            FunctionalDependency fd;
            if (r instanceof FunctionalDependency) {
                fd = (FunctionalDependency) r;
            } else if (r instanceof RelaxedFunctionalDependency) {
                fd = (RelaxedFunctionalDependency) r;
            } else {
                // ignore unknown result types
                continue;
            }
            ColumnCombination det = fd.getDeterminant();
            ColumnIdentifier dep = fd.getDependant();
            out.add(new FDKey(new ArrayList<>(det.getColumnIdentifiers()), dep));
        }
        return out;
    }
}

