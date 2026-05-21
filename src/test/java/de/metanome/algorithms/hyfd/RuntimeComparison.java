package de.metanome.algorithms.hyfd;

import de.metanome.algorithm_integration.results.Result;

import java.io.File;
import java.util.List;

public class RuntimeComparison extends Runner {

    public static void main(String[] args) {
        File csvFile = new File("../../../../../data/hospital/clean/clean.csv");

        if (!csvFile.exists()) {
            throw new RuntimeException("CSV file does not exist: " + csvFile.getAbsolutePath());
        }

        Runner.path = csvFile.getParent();

        System.out.println("Running HyFD on: " + csvFile.getAbsolutePath());

        double threshold = 1.0d;

        long startTime = System.nanoTime();

        List<Result> hyfdResults = executeHyFD(HyFD.Mode.GPDEP, threshold, csvFile.getName());

        long endTime = System.nanoTime();

        long durationNs = endTime - startTime;
        double durationMs = durationNs / 1_000_000.0;
        double durationSeconds = durationNs / 1_000_000_000.0;

        System.out.println("HyFD execution time: " + durationMs + " ms");
        System.out.println("HyFD execution time: " + durationSeconds + " seconds");

        System.out.println("HyFD results: " + hyfdResults.size());

        for (Result result : hyfdResults) {
            System.out.println(result);
        }
    }
}
