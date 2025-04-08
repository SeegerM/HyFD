import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Runner {

    static String path = "C://Users/MarcianSeeger/Documents/HyFD/data";

    public static void main2(String[] args) {
        File[] csvFiles = getCsvFiles(Runner.path, "hospital_dirty.csv");
        String[] fileNames = Arrays.stream(csvFiles).map(File::getName).toArray(String[]::new);

        System.out.println("Start Execution");
        List<Result> results = executeHyFD(fileNames);

        System.out.println("Start GPDEP Score Execution");
        List<Pair<RelaxedFunctionalDependency, PdepTuple>> result = MetadataUtils.getPdeps(results, csvFiles);
        result.sort(Comparator.comparingDouble(p -> p.getSecond().gpdep));

        Map<Integer, String> indexToName = new HashMap<>();
        Map<String, List<Integer>> rawViolations = readViolationMap();
        Map<String, List<Integer>> renamedViolations = renameViolations(rawViolations, indexToName);
        Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations = filterAcceptedViolations(result, renamedViolations);

        printGpdepStats(result);

        Map<String, RelaxedFunctionalDependency> bestPerRhs = getBestRFDsPerRhs(acceptedViolations);
        Map<String, List<RelaxedFunctionalDependency>> rfdsPerRhs = groupRFDsPerRhs(acceptedViolations);
        Map<String, List<List<Integer>>> violationsPerAttributeInFile = readXIndicesColumnWise(csvFiles);

        System.out.println("\n--- Evaluation (Best RFD per RHS) ---");
        ScoreSummary bestScore = evaluateBestRFDs(bestPerRhs, acceptedViolations, violationsPerAttributeInFile, indexToName, fileNames[0]);
        printOverallF1(bestScore, violationsPerAttributeInFile.get(fileNames[0]), indexToName, bestPerRhs, "All RHS from ground truth");

        System.out.println("\n--- Evaluation (Voting across all RFDs per RHS) ---");
        ScoreSummary votingScore = evaluateVotingBasedRFDs(rfdsPerRhs, acceptedViolations, violationsPerAttributeInFile, indexToName, fileNames[0]);
        printOverallF1(votingScore, violationsPerAttributeInFile.get(fileNames[0]), indexToName, bestPerRhs, "All RHS from ground truth (Voting)");

        clean();
    }

    private static File[] getCsvFiles(String path, String dataset) {
        File folder = new File(path);
        File[] csvFiles = folder.listFiles((dir, name) -> name.equalsIgnoreCase(dataset));
        return csvFiles;
    }


    static Map<String, List<Integer>> renameViolations(Map<String, List<Integer>> violations, Map<Integer, String> indexToName) {
        return violations.keySet().stream()
                .collect(Collectors.toMap(
                        k -> {
                            String[] split = k.replace("FD(", "").replace(")", "").split("->");
                            String[] leftSplit = split[0].split(",");
                            String[] rightSplit = split[1].split(",");
                            indexToName.put(Integer.parseInt(leftSplit[0]), leftSplit[1]);
                            indexToName.put(Integer.parseInt(rightSplit[0]), rightSplit[1]);
                            return leftSplit[1] + "->" + rightSplit[1];
                        },
                        violations::get
                ));
    }


    public static Map<RelaxedFunctionalDependency, List<Integer>> filterAcceptedViolations(List<Pair<RelaxedFunctionalDependency, PdepTuple>> results,
                                                                                           Map<String, List<Integer>> renamedViolations) {
        Map<RelaxedFunctionalDependency, List<Integer>> accepted = new HashMap<>();
        for (Pair<RelaxedFunctionalDependency, PdepTuple> pair : results) {
            RelaxedFunctionalDependency fd = pair.getFirst();
            PdepTuple scores = pair.getSecond();
            if (scores.gpdep > 0.5) {
                String key = fd.getDeterminant().toString().replace("[", "").replace("]", "") + "->" + fd.getDependant();
                accepted.put(fd, renamedViolations.get(key));
            }
        }
        return accepted;
    }

    public static void printGpdepStats(List<Pair<RelaxedFunctionalDependency, PdepTuple>> results) {
        Map<String, List<Double>> gpdepByColumn = new HashMap<>();
        double totalGpdep = 0.0;
        int totalCount = 0;

        for (Pair<RelaxedFunctionalDependency, PdepTuple> pair : results) {
            RelaxedFunctionalDependency fd = pair.getFirst();
            PdepTuple scores = pair.getSecond();
            String dependent = fd.getDependant().getColumnIdentifier();
            if (scores.gpdep > 0.5) {
                gpdepByColumn.computeIfAbsent(dependent, k -> new ArrayList<>()).add(scores.gpdep);
                totalGpdep += scores.gpdep;
                totalCount++;
            }
        }

        System.out.println("\nAverage gpdep per dependent column:");
        for (Map.Entry<String, List<Double>> entry : gpdepByColumn.entrySet()) {
            String column = entry.getKey();
            List<Double> gpdepList = entry.getValue();
            double sum = gpdepList.stream().mapToDouble(Double::doubleValue).sum();
            double average = gpdepList.isEmpty() ? 1.0 : sum / gpdepList.size();
            System.out.printf("Column: %-20s  Avg gpdep: %.4f%n", column, average);
        }

        double overallAverage = totalCount == 0 ? 0.0 : totalGpdep / totalCount;
        System.out.printf("\nOverall average gpdep: %.4f%n", overallAverage);
    }

    public static Map<String, RelaxedFunctionalDependency> getBestRFDsPerRhs(Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations) {
        Map<String, RelaxedFunctionalDependency> best = new HashMap<>();
        for (RelaxedFunctionalDependency rfd : acceptedViolations.keySet()) {
            String rhs = rfd.getDependant().toString();
            best.merge(rhs, rfd, (existing, current) -> current.getMeasure() > existing.getMeasure() ? current : existing);
        }
        return best;
    }

    public static Map<String, List<RelaxedFunctionalDependency>> groupRFDsPerRhs(Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations) {
        Map<String, List<RelaxedFunctionalDependency>> grouped = new HashMap<>();
        for (RelaxedFunctionalDependency rfd : acceptedViolations.keySet()) {
            String rhs = rfd.getDependant().toString();
            grouped.computeIfAbsent(rhs, k -> new ArrayList<>()).add(rfd);
        }
        return grouped;
    }

    public static ScoreSummary evaluateBestRFDs(Map<String, RelaxedFunctionalDependency> bestPerRhs,
                                                Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations,
                                                Map<String, List<List<Integer>>> violationsPerFile,
                                                Map<Integer, String> indexToName,
                                                String fileName) {

        int tp = 0, fp = 0, fn = 0;
        for (Map.Entry<String, RelaxedFunctionalDependency> entry : bestPerRhs.entrySet()) {
            String rhs = entry.getKey();
            RelaxedFunctionalDependency rfd = entry.getValue();
            if (rfd.getDeterminant().getColumnIdentifiers().isEmpty()) continue;

            int index = getIndex(indexToName, rhs);
            List<Integer> expected = violationsPerFile.get(fileName).get(index);
            List<Integer> actual = acceptedViolations.get(rfd);

            Set<Integer> expectedSet = new HashSet<>(expected);
            Set<Integer> actualSet = new HashSet<>(actual);

            Set<Integer> tpSet = new HashSet<>(actualSet);
            tpSet.retainAll(expectedSet);

            int localTP = tpSet.size();
            int localFP = actualSet.size() - localTP;
            int localFN = expectedSet.size() - localTP;

            tp += localTP;
            fp += localFP;
            fn += localFN;

            double f1 = computeF1(expectedSet, actualSet);
            System.out.printf("%-70s (F1): %.3f, Measure: %.3f%n", rfd.getDeterminant() + "->" + rfd.getDependant(), f1, rfd.getMeasure());
        }
        return new ScoreSummary(tp, fp, fn, computeF1(tp, fp, fn));
    }

    public static ScoreSummary evaluateVotingBasedRFDs(Map<String, List<RelaxedFunctionalDependency>> rfdsPerRhs,
                                                       Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations,
                                                       Map<String, List<List<Integer>>> violationsPerFile,
                                                       Map<Integer, String> indexToName,
                                                       String fileName) {

        int tp = 0, fp = 0, fn = 0;
        for (Map.Entry<String, List<RelaxedFunctionalDependency>> entry : rfdsPerRhs.entrySet()) {
            String rhs = entry.getKey();
            List<RelaxedFunctionalDependency> rfds = entry.getValue();
            if (rfds.stream().allMatch(rfd -> rfd.getDeterminant().getColumnIdentifiers().isEmpty())) continue;

            Map<Integer, Integer> indexCount = new HashMap<>();
            for (RelaxedFunctionalDependency rfd : rfds) {
                List<Integer> violations = acceptedViolations.get(rfd);
                for (Integer index : violations) {
                    indexCount.merge(index, 1, Integer::sum);
                }
            }

            int threshold = rfds.size() / 2;
            Set<Integer> actualSet = indexCount.entrySet().stream()
                    .filter(e -> e.getValue() > threshold)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            int index = getIndex(indexToName, rhs);
            List<Integer> expected = violationsPerFile.get(fileName).get(index);
            Set<Integer> expectedSet = new HashSet<>(expected);

            Set<Integer> tpSet = new HashSet<>(actualSet);
            tpSet.retainAll(expectedSet);

            int localTP = tpSet.size();
            int localFP = actualSet.size() - localTP;
            int localFN = expectedSet.size() - localTP;

            tp += localTP;
            fp += localFP;
            fn += localFN;

            double f1 = computeF1(expectedSet, actualSet);
            System.out.printf("%-70s (F1): %.3f, Voted from %d FDs%n", "VOTED -> " + rhs, f1, rfds.size());
        }
        return new ScoreSummary(tp, fp, fn, computeF1(tp, fp, fn));
    }

    public static void printOverallF1(ScoreSummary score, List<List<Integer>> expectedViolationsPerColumn,
                                      Map<Integer, String> indexToName,
                                      Map<String, RelaxedFunctionalDependency> bestPerRhs,
                                      String label) {

        int tp = score.tp(), fp = score.fp(), fn = score.fn();

        for (int i = 0; i < expectedViolationsPerColumn.size(); i++) {
            String rhs = indexToName.get(i);
            if (!bestPerRhs.containsKey(rhs)) {
                fn += expectedViolationsPerColumn.get(i).size();
            }
        }

        double precision = tp + fp == 0 ? 0 : (double) tp / (tp + fp);
        double recall = tp + fn == 0 ? 0 : (double) tp / (tp + fn);
        double f1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

        System.out.printf("%-80s (Overall F1): %.3f%n", label, f1);
    }

    public static double computeF1(Set<Integer> expected, Set<Integer> actual) {
        Set<Integer> tpSet = new HashSet<>(actual);
        tpSet.retainAll(expected);

        int tp = tpSet.size();
        int fp = actual.size() - tp;
        int fn = expected.size() - tp;

        return computeF1(tp, fp, fn);
    }

    public static double computeF1(int tp, int fp, int fn) {
        double precision = tp + fp == 0 ? 0 : (double) tp / (tp + fp);
        double recall = tp + fn == 0 ? 0 : (double) tp / (tp + fn);
        return precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);
    }
    
    public static void main(String[] args) {
        File folder = new File(Runner.path);
        File[] csvFiles = folder.listFiles((dir, name) -> name.equalsIgnoreCase("hospital_dirty.csv"));

        // Extract file names from the CSV files.
        String[] fileNames = new String[csvFiles.length];
        for (int i = 0; i < csvFiles.length; i++) {
            fileNames[i] = csvFiles[i].getName();
        }

        System.out.println("Start Execution");
        // Execute HyFD on all found CSV files.
        List<Result> results = executeHyFD(fileNames);

        System.out.println("Start GPDEP Score Execution");
        List<Pair<RelaxedFunctionalDependency, PdepTuple>> result = MetadataUtils.getPdeps(results, csvFiles);

        result.sort(Comparator.comparingDouble(pair -> pair.getSecond().gpdep));

        Map<String, List<Double>> gpdepByColumn = new HashMap<>();

        double totalGpdep = 0.0;
        int totalCount = 0;


        Map<Integer, String> indexToName = new HashMap<>();
        Map<String, List<Integer>> violations = readViolationMap();
        Map<RelaxedFunctionalDependency, List<Integer>> acceptedViolations = new HashMap<>();
        Map<String, List<Integer>> newViolations = violations.keySet().stream()
                .collect(Collectors.toMap(
                        k -> {
                            String[] split = k.replace("FD(", "").replace(")", "").split("->");
                            String[] leftSplit = split[0].split(",");
                            String[] rightSplit = split[1].split(",");
                            indexToName.put(Integer.valueOf(leftSplit[0]), leftSplit[1]);
                            indexToName.put(Integer.valueOf(rightSplit[0]), rightSplit[1]);
                            return leftSplit[1] + "->" + rightSplit[1];
                        },
                        k -> violations.get(k)
                ));

        for (Pair<RelaxedFunctionalDependency, PdepTuple> pair : result) {
            //System.out.println("FD: " + pair.getFirst() + ", pdep: " + pair.getSecond().pdep + ", gpdep: " + pair.getSecond().gpdep);
            RelaxedFunctionalDependency fd = pair.getFirst();
            PdepTuple scores = pair.getSecond();

            String dependent = fd.getDependant().getColumnIdentifier();
            System.out.println(fd.getDeterminant() + "->" + fd.getDependant() + " #" + scores.gpdep);
            if (scores.gpdep > 0.5d){
                gpdepByColumn
                        .computeIfAbsent(dependent, k -> new ArrayList<>())
                        .add(scores.gpdep);
                totalGpdep += scores.gpdep;
                totalCount++;
                String key = pair.getFirst().getDeterminant().toString().replace("[", "").replace("]","") + "->" + pair.getFirst().getDependant();
                if (!newViolations.containsKey(key))
                    System.out.println("");
                fd.setMeasure(scores.gpdep);
                acceptedViolations.put(fd, newViolations.get(key));
            }
        }

        System.out.println("\nAverage gpdep per dependent column:");
        for (Map.Entry<String, List<Double>> entry : gpdepByColumn.entrySet()) {
            String column = entry.getKey();
            List<Double> gpdepList = entry.getValue();

            double sum = 0.0;
            for (double gpdep : gpdepList) {
                sum += gpdep;
            }
            double average = gpdepList.isEmpty() ? 1.0 : sum / gpdepList.size();

            System.out.printf("Column: %-20s  Avg gpdep: %.4f%n", column, average);
        }

        double overallAverage = totalCount == 0 ? 0.0 : totalGpdep / totalCount;
        System.out.printf("\nOverall average gpdep: %.4f%n", overallAverage);
        System.out.println();

        Map<String, RelaxedFunctionalDependency> bestPerRhs = new HashMap<>();

        for (RelaxedFunctionalDependency rfd : acceptedViolations.keySet()) {
            String rhs = rfd.getDependant().toString();
            bestPerRhs.merge(rhs, rfd, (existing, current) ->
                    current.getMeasure() > existing.getMeasure() ? current : existing
            );
        }

        Map<String, List<RelaxedFunctionalDependency>> rfdsPerRhs = new HashMap<>();

        for (RelaxedFunctionalDependency rfd : acceptedViolations.keySet()) {
            String rhs = rfd.getDependant().toString();
            rfdsPerRhs.computeIfAbsent(rhs, k -> new ArrayList<>()).add(rfd);
        }

        int totalTP = 0;
        int totalFP = 0;
        int totalFN = 0;

        Map<String, List<List<Integer>>> violationsPerAttributeInFile = readXIndicesColumnWise(csvFiles);
        for (Map.Entry<String, RelaxedFunctionalDependency> entry : bestPerRhs.entrySet()) {
            String rhs = entry.getKey();
            RelaxedFunctionalDependency bestRfd = entry.getValue();
            if (entry.getValue().getDeterminant().getColumnIdentifiers().isEmpty())
                continue;
            Integer indexOfRhs = getIndex(indexToName, rhs);
            List<Integer> expectedViolations = violationsPerAttributeInFile.get(fileNames[0]).get(indexOfRhs);
            List<Integer> actualViolations = acceptedViolations.get(bestRfd);

            Set<Integer> expectedSet = new HashSet<>(expectedViolations);
            Set<Integer> actualSet = new HashSet<>(actualViolations);

            Set<Integer> tpSet = new HashSet<>(actualSet);
            tpSet.retainAll(expectedSet);

            int tp = tpSet.size();
            int fp = actualSet.size() - tp;
            int fn = expectedSet.size() - tp;

            totalTP += tp;
            totalFP += fp;
            totalFN += fn;

            double f1 = computeF1(expectedViolations, actualViolations);
            System.out.printf("%-70s (F1): %.3f, GPDEP: %.3f%n", bestRfd.getDeterminant() + "->" + bestRfd.getDependant(), f1, bestRfd.getMeasure());
        }


        double precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
        double recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
        double overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

        System.out.printf("%n%-80s (Overall F1): %.3f%n", "All RHS Combined", overallF1);

        // Step 2: Handle RHS attributes not covered by acceptedViolations
        List<List<Integer>> expectedViolationsPerColumn = violationsPerAttributeInFile.get(fileNames[0]);
        int numColumns = expectedViolationsPerColumn.size();

        for (int colIndex = 0; colIndex < numColumns; colIndex++) {
            String rhs = indexToName.get(colIndex);
            if (!bestPerRhs.containsKey(rhs)) {
                List<Integer> expected = expectedViolationsPerColumn.get(colIndex);
                totalFN += expected.size(); // nothing predicted → all expected are FN
            }
        }

        precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
        recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
        overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

        System.out.printf("%-80s (Overall F1 - all RHS): %.3f%n", "All RHS from ground truth", overallF1);
        System.out.println("");

        totalTP = 0;
        totalFP = 0;
        totalFN = 0;

        for (Map.Entry<String, List<RelaxedFunctionalDependency>> entry : rfdsPerRhs.entrySet()) {
            String rhs = entry.getKey();
            List<RelaxedFunctionalDependency> rfds = entry.getValue();
            int size = rfds.size();

            // Skip if all determinants are empty
            if (rfds.stream().allMatch(rfd -> rfd.getDeterminant().getColumnIdentifiers().isEmpty())) {
                continue;
            }

            if (rfds.size() == 2) { //No majority "possible" will take all values therefore revert back to take the best
                if (rfds.get(0).getMeasure() > rfds.get(1).getMeasure())
                    rfds.remove(1);
                else
                    rfds.remove(0);
            }

            // Step 2: Majority voting on actual violations
            Map<Integer, Integer> indexCount = new HashMap<>();
            for (RelaxedFunctionalDependency rfd : rfds) {
                List<Integer> violations2 = acceptedViolations.get(rfd);
                for (Integer index : violations2) {
                    indexCount.merge(index, 1, Integer::sum);
                }
            }

            int threshold = (rfds.size() / 2);
            Set<Integer> actualSet = indexCount.entrySet().stream()
                    .filter(e -> e.getValue() > threshold)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Integer indexOfRhs = getIndex(indexToName, rhs);
            List<Integer> expectedViolations = violationsPerAttributeInFile.get(fileNames[0]).get(indexOfRhs);
            Set<Integer> expectedSet = new HashSet<>(expectedViolations);

            // Compute TP, FP, FN
            Set<Integer> tpSet = new HashSet<>(actualSet);
            tpSet.retainAll(expectedSet);

            //_------
            Set<Integer> onlyInOne = new HashSet<>(actualSet);
            onlyInOne.addAll(expectedSet); // now has union

            Set<Integer> intersection = new HashSet<>(actualSet);
            intersection.retainAll(expectedSet); // now has common elements

            onlyInOne.removeAll(intersection);
            System.out.print("");
            //-----------


            int tp = tpSet.size();
            int fp = actualSet.size() - tp;
            int fn = expectedSet.size() - tp;

            totalTP += tp;
            totalFP += fp;
            totalFN += fn;

            double f1 = computeF1(expectedViolations, new ArrayList<>(actualSet));
            System.out.printf("%-70s (F1): %.3f, Voted from %d FDs%n", "VOTED -> " + rhs, f1, size);
        }

        precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
        recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
        overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

        System.out.printf("%n%-80s (Overall F1): %.3f%n", "All RHS Combined (Voting)", overallF1);

        for (int colIndex = 0; colIndex < numColumns; colIndex++) {
            String rhs = indexToName.get(colIndex);
            if (!bestPerRhs.containsKey(rhs)) {
                List<Integer> expected = expectedViolationsPerColumn.get(colIndex);
                totalFN += expected.size(); // nothing predicted → all expected are FN
            }
        }

        precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
        recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
        overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

        System.out.printf("%-80s (Overall F1 - all RHS): %.3f%n", "All RHS from ground truth (Voting)", overallF1);

        clean();
    }

    private static void clean() {
        File tempFile = new File(System.getProperty("java.io.tmpdir"), "violations_temp.txt");
        if (tempFile.exists()) {
            tempFile.delete(); // Deletes the file
            try {
                tempFile.createNewFile(); // Creates an empty one again
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static double computeF1(List<Integer> expected, List<Integer> actual) {
        Set<Integer> expectedSet = new HashSet<>(expected);
        Set<Integer> actualSet = new HashSet<>(actual);

        Set<Integer> tpSet = new HashSet<>(actualSet);
        tpSet.retainAll(expectedSet); // TP

        int tp = tpSet.size();
        int fp = actualSet.size() - tp;
        int fn = expectedSet.size() - tp;

        double precision = tp + fp == 0 ? 0 : (double) tp / (tp + fp);
        double recall = tp + fn == 0 ? 0 : (double) tp / (tp + fn);

        return precision + recall == 0 ? 0 : 2 * (precision * recall) / (precision + recall);
    }

    private static Integer getIndex(Map<Integer, String> getNames, String rhs) {
        return getNames.entrySet().stream()
                .filter(entry -> rhs.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }


    public static List<Result> executeHyFD(String... names) {
        List<Result> allResults = new ArrayList<>();
        try {
            for (String fileName : names) {
                RelationalInputGenerator input = getInputGenerator(fileName);
                ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(input));

                HyFD hyFD = createHyFD(input, resultReceiver);

                long time = System.currentTimeMillis();
                hyFD.execute();
                time = System.currentTimeMillis() - time;

                List<Result> results = resultReceiver.fetchNewResults();
                allResults.addAll(results);
            }
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
        return allResults;
    }

    public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();
        for (String columnName : relationalInput.columnNames())
            acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
        return acceptedColumns;
    }

    public static String getFileInputPath(String fileName) {
        return Runner.path + File.separator + fileName;
    }

    public static RelationalInputGenerator getInputGenerator(String fileName) throws AlgorithmConfigurationException {
        return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                getFileInputPath(fileName),
                true,
                ',',
                '"',
                '\\',
                false,
                true,
                0,
                true,
                true,
                ""
        ));
    }

    public static HyFD createHyFD(RelationalInputGenerator input, ResultCache resultReceiver) throws AlgorithmConfigurationException {
        HyFD hyFD = new HyFD();
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
        hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), "0.8");
        hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), 1);
        hyFD.setResultReceiver(resultReceiver);
        return hyFD;
    }

    private static Map<Integer, String> getNamesOfAttributes(String[] paths) {
        Map<Integer, String> nameMap = new HashMap<>();
        int index = 0;
        for (String path : paths) {
            String fileName = path.substring(path.lastIndexOf("/") + 1);
            List<String> names = new ArrayList<>();
            try {
                RelationalInputGenerator input = getInputGenerator(path);
                names = input.generateNewCopy().columnNames();
            } catch (InputGenerationException e) {
                throw new RuntimeException(e);
            } catch (AlgorithmConfigurationException e) {
                throw new RuntimeException(e);
            }
            for (String name : names) {
                nameMap.put(index, fileName + "." + name);
                index++;
            }
        }
        return nameMap;
    }
    public static Map<String, List<Integer>> readViolationMap() {
        Map<String, List<Integer>> fdViolations = new HashMap<>();
        // Locate the temporary file in the system's temp directory.
        File tempFile = new File(System.getProperty("java.io.tmpdir"), "violations_temp.txt");

        // If the file does not exist, return an empty map.
        if (!tempFile.exists()) {
            return fdViolations;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(tempFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line around the colon. Expected to produce two parts.
                String[] parts = line.split(":");
                if (parts.length < 2) {
                    continue;  // skip if the format is invalid
                }

                // The FD is the part before the colon (e.g., "FD(1->2)")
                String fd = parts[0].trim();
                // The list of violation record IDs is after the colon.
                String violationsPart = parts[1].trim();
                List<Integer> violationList = new ArrayList<>();

                // If there are any numbers listed, split by comma and parse them
                if (!violationsPart.isEmpty()) {
                    String[] idStrings = violationsPart.split(",");
                    for (String idStr : idStrings) {
                        try {
                            int id = Integer.parseInt(idStr.trim());
                            violationList.add(id);
                        } catch (NumberFormatException e) {
                            // Here you could log the error if needed, or simply ignore the malformed value.
                        }
                    }
                }
                // Put the FD and its associated violation record IDs into the map.
                fdViolations.put(fd, violationList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fdViolations;
    }

    public static Map<String, List<List<Integer>>> readXIndicesColumnWise(File[] csvFiles) {
        Map<String, List<List<Integer>>> tableMap = new HashMap<>();

        for (File file : csvFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                List<List<String>> rows = reader.lines()
                        .map(line -> splitCSVRespectingQuotes(line)) //.split(",")
                        .collect(Collectors.toList());

                if (rows.isEmpty()) {
                    tableMap.put(file.getName(), new ArrayList<>());
                    continue;
                }

                int columnCount = rows.get(0).size();
                List<List<Integer>> columnWiseXIndices = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    columnWiseXIndices.add(new ArrayList<>());
                }

                for (int rowIndex = 1; rowIndex < rows.size(); rowIndex++) {
                    List<String> row = rows.get(rowIndex);
                    for (int col = 0; col < row.size(); col++) {
                        if (row.get(col).trim().contains("x")) {
                            columnWiseXIndices.get(col).add(rowIndex-1); //Header is not counted in HyFD Indexing therefore -1
                        }
                    }
                }

                tableMap.put(file.getName(), columnWiseXIndices);

            } catch (IOException e) {
                System.err.println("Error reading file: " + file.getName());
                e.printStackTrace();
            }
        }

        return tableMap;
    }

    public static List<String> splitCSVRespectingQuotes(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Toggle quoted mode
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                // If comma is outside quotes, it's a delimiter
                result.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // Add the last field
        result.add(current.toString().trim());

        return result;
    }

}


