package de.metanome.algorithms.hyfd;

import de.metanome.algorithm_integration.results.Result;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class RuntimeComparison extends Runner {

    private static final String[] DATASETS = {
            "allergen",
            "beers",
            "eudract",
            "flights",
            "hospital",
            "rayyan",
            "tax"
    };

    private static final HyFD.Mode[] MODES = {
            HyFD.Mode.GPDEP,
            HyFD.Mode.STANDARD
    };

    private static final double THRESHOLD = 1.0d;

    public static void main(String[] args) {
        int runs = 3;

        if (args.length > 0) {
            runs = Integer.parseInt(args[0]);
        }

        if (runs <= 0) {
            throw new IllegalArgumentException("Number of runs must be greater than 0.");
        }

        File runOutputFile = new File("../experiments/results/hyfd_runtime_runs.csv");
        File summaryOutputFile = new File("../experiments/results/hyfd_runtime_summary.csv");

        try (
                PrintWriter runWriter = new PrintWriter(new FileWriter(runOutputFile));
                PrintWriter summaryWriter = new PrintWriter(new FileWriter(summaryOutputFile))
        ) {
            runWriter.println("dataset,mode,run,result_count,duration_ns,duration_ms,duration_seconds");

            summaryWriter.println(
                    "dataset," +
                            "gpdep_avg_ms," +
                            "standard_avg_ms," +
                            "difference_ms," +
                            "difference_seconds," +
                            "difference_percent," +
                            "faster_mode"
            );

            for (String dataset : DATASETS) {
                File csvFile = new File("../data/" + dataset + "/clean/clean.csv");

                if (!csvFile.exists()) {
                    System.err.println("Skipping dataset '" + dataset + "': CSV file does not exist: "
                            + csvFile.getAbsolutePath());
                    continue;
                }

                Runner.path = csvFile.getParent();

                System.out.println();
                System.out.println("========================================");
                System.out.println("Dataset: " + dataset);
                System.out.println("CSV: " + csvFile.getAbsolutePath());
                System.out.println("Runs per mode: " + runs);
                System.out.println("========================================");

                Map<HyFD.Mode, Long> totalDurationNsByMode = new EnumMap<>(HyFD.Mode.class);
                Map<HyFD.Mode, Integer> lastResultCountByMode = new EnumMap<>(HyFD.Mode.class);

                for (HyFD.Mode mode : MODES) {
                    totalDurationNsByMode.put(mode, 0L);

                    for (int run = 1; run <= runs; run++) {
                        System.out.println("Running HyFD mode " + mode + ", run " + run + "/" + runs);

                        long startTime = System.nanoTime();

                        List<Result> hyfdResults = executeHyFD(
                                mode,
                                THRESHOLD,
                                csvFile.getName()
                        );

                        long endTime = System.nanoTime();

                        long durationNs = endTime - startTime;
                        double durationMs = durationNs / 1_000_000.0;
                        double durationSeconds = durationNs / 1_000_000_000.0;

                        int resultCount = hyfdResults.size();

                        totalDurationNsByMode.put(
                                mode,
                                totalDurationNsByMode.get(mode) + durationNs
                        );

                        lastResultCountByMode.put(mode, resultCount);

                        System.out.println("Results: " + resultCount);
                        System.out.println("Execution time: " + durationMs + " ms");
                        System.out.println("Execution time: " + durationSeconds + " seconds");

                        runWriter.printf(
                                "%s,%s,%d,%d,%d,%.3f,%.6f%n",
                                dataset,
                                mode,
                                run,
                                resultCount,
                                durationNs,
                                durationMs,
                                durationSeconds
                        );

                        runWriter.flush();
                    }
                }

                double gpdepAvgNs = totalDurationNsByMode.get(HyFD.Mode.GPDEP) / (double) runs;
                double standardAvgNs = totalDurationNsByMode.get(HyFD.Mode.STANDARD) / (double) runs;

                double gpdepAvgMs = gpdepAvgNs / 1_000_000.0;
                double standardAvgMs = standardAvgNs / 1_000_000.0;

                double differenceMs = standardAvgMs - gpdepAvgMs;
                double differenceSeconds = differenceMs / 1_000.0;

                double differencePercent;
                if (standardAvgMs == 0.0) {
                    differencePercent = 0.0;
                } else {
                    differencePercent = (differenceMs / standardAvgMs) * 100.0;
                }

                String fasterMode;
                if (gpdepAvgMs < standardAvgMs) {
                    fasterMode = HyFD.Mode.GPDEP.toString();
                } else if (standardAvgMs < gpdepAvgMs) {
                    fasterMode = HyFD.Mode.STANDARD.toString();
                } else {
                    fasterMode = "equal";
                }

                System.out.println();
                System.out.println("Average results for dataset: " + dataset);
                System.out.println("GPDEP average:    " + gpdepAvgMs + " ms");
                System.out.println("STANDARD average: " + standardAvgMs + " ms");
                System.out.println("Difference STANDARD - GPDEP: " + differenceMs + " ms");
                System.out.println("Difference percent relative to STANDARD: " + differencePercent + " %");
                System.out.println("Faster mode: " + fasterMode);

                summaryWriter.printf(
                        "%s,%.3f,%.3f,%.3f,%.6f,%.3f,%s%n",
                        dataset,
                        gpdepAvgMs,
                        standardAvgMs,
                        differenceMs,
                        differenceSeconds,
                        differencePercent,
                        fasterMode
                );

                summaryWriter.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println();
        System.out.println("Detailed run results written to: " + runOutputFile.getAbsolutePath());
        System.out.println("Average summary written to: " + summaryOutputFile.getAbsolutePath());
    }
}
