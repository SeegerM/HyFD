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
    public static void main(String[] args) {
        File folder = new File(Runner.path);
        File[] csvFiles = folder.listFiles((dir, name) -> name.equalsIgnoreCase("hospital_dirty.csv"));

        // Extract file names from the CSV files.
        String[] fileNames = new String[csvFiles.length];
        for (int i = 0; i < csvFiles.length; i++) {
            fileNames[i] = csvFiles[i].getName();
        }
        System.out.println("Threshold,F1BestLocal,F1Best,F1VoteLocal,F1Vote");
        for (int i = 10; i >= 1; i--) {
            double value = i / 100.0;
            System.out.print("" + value);

            //System.out.println("Start Execution");
            // Execute HyFD on all found CSV files.
            List<Result> results = executeHyFD(value, fileNames);

            //for (Result printR : results){
            //    System.out.println(printR);
            //}
            //System.out.println("----------------------");
            //System.out.println("Start GPDEP Score Execution");
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
                                String parsedFDString = parseFd(k, indexToName);
                                return parsedFDString;
                            },
                            k -> violations.get(k)
                    ));

            for (Pair<RelaxedFunctionalDependency, PdepTuple> pair : result) {
                //System.out.println("FD: " + pair.getFirst() + ", pdep: " + pair.getSecond().pdep + ", gpdep: " + pair.getSecond().gpdep);
                RelaxedFunctionalDependency fd = pair.getFirst();
                PdepTuple scores = pair.getSecond();

                String dependent = fd.getDependant().getColumnIdentifier();
                //System.out.println(fd.getDeterminant() + "->" + fd.getDependant() + " #" + scores.gpdep);
                if (scores.gpdep > 0.5d){
                    gpdepByColumn
                            .computeIfAbsent(dependent, k -> new ArrayList<>())
                            .add(scores.gpdep);
                    totalGpdep += scores.gpdep;
                    totalCount++;
                    //if (fd.toString().contains("[hospital_dirty.csv.measure_code, hospital_dirty.csv.state]->hospital_dirty.csv.state_average"))
                    //    System.out.println("");
                    String key = pair.getFirst().getDeterminant().toString().replace("[", "").replace("]","") + "->" + pair.getFirst().getDependant();
                    if (!newViolations.containsKey(key)){
                    //    if (!pair.getFirst().getDeterminant().getColumnIdentifiers().isEmpty())
                    //        System.out.println("Not in the map but as a violation! " + fd);
                        key = "[]" + "->" + pair.getFirst().getDependant();
                    //    if (!newViolations.containsKey(key))
                     //       System.out.println("");
                    }
                    fd.setMeasure(scores.gpdep);
                    acceptedViolations.put(fd, newViolations.get(key));
                }
            }

            //System.out.println("\nAverage gpdep per dependent column:");
            for (Map.Entry<String, List<Double>> entry : gpdepByColumn.entrySet()) {
                String column = entry.getKey();
                List<Double> gpdepList = entry.getValue();

                double sum = 0.0;
                for (double gpdep : gpdepList) {
                    sum += gpdep;
                }
                double average = gpdepList.isEmpty() ? 1.0 : sum / gpdepList.size();

                //System.out.printf("Column: %-20s  Avg gpdep: %.4f%n", column, average);
            }

            double overallAverage = totalCount == 0 ? 0.0 : totalGpdep / totalCount;
            //System.out.printf("\nOverall average gpdep: %.4f%n", overallAverage);
            //System.out.println();

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
                if (actualViolations == null) {
                    actualViolations = new ArrayList<>();
                }
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
                //System.out.printf("%-70s (F1): %.3f, GPDEP: %.3f%n", bestRfd.getDeterminant() + "->" + bestRfd.getDependant(), f1, bestRfd.getMeasure());
            }


            double precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
            double recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
            double overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

            System.out.print("," + overallF1);
            //System.out.printf("%n%-80s (Overall F1): %.3f%n", "All RHS Combined", overallF1);

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

            //System.out.printf("%-80s (Overall F1 - all RHS): %.3f%n", "All RHS from ground truth", overallF1);
            //System.out.println("");
            System.out.print("," + overallF1);

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
                    if (violations2 == null)
                        violations2 = new ArrayList<>();
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
                //System.out.print("");
                //-----------


                int tp = tpSet.size();
                int fp = actualSet.size() - tp;
                int fn = expectedSet.size() - tp;

                totalTP += tp;
                totalFP += fp;
                totalFN += fn;

                double f1 = computeF1(expectedViolations, new ArrayList<>(actualSet));
                //System.out.printf("%-70s (F1): %.3f, Voted from %d FDs%n", "VOTED -> " + rhs, f1, size);
            }

            precision = totalTP + totalFP == 0 ? 0 : (double) totalTP / (totalTP + totalFP);
            recall = totalTP + totalFN == 0 ? 0 : (double) totalTP / (totalTP + totalFN);
            overallF1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);

            //System.out.printf("%n%-80s (Overall F1): %.3f%n", "All RHS Combined (Voting)", overallF1);
            System.out.print("," + overallF1);
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

            //System.out.printf("%-70s (Overall F1 - all RHS): %.3f%n", "All RHS from ground truth (Voting)", overallF1);
            //System.out.printf("%-70s (Overall Recall - all RHS): %.3f%n", "All RHS from ground truth (Voting)", recall);
            //System.out.printf("%-70s (Overall Precision - all RHS): %.3f%n", "All RHS from ground truth (Voting)", precision);
            System.out.print("," + overallF1 + "\n");
            clean();
        }
    }

    private static String parseFd(String k, Map<Integer, String> indexToName) {
        // Expected format: FD(1,2,3,A,B,C->4,D): <violation details>
        int start = k.indexOf("FD(");
        int end = k.indexOf(")");
        if (start < 0 || end < 0) {
            System.err.println("Problem");
            return null;
        }

        // Extract the header part inside FD( ... ):
        String header = k.substring(start + 3, end); // e.g., "1,2,3,A,B,C->4,D"

        // Split into left-hand side and right-hand side parts using "->"
        String[] parts = header.split("->");
        if (parts.length != 2) {
            System.err.println("Problem");
            return null;
        }
        String leftPart = parts[0];   // e.g., "1,2,3,A,B,C"
        String rightPart = parts[1];  // e.g., "4,D"

        // Split leftPart tokens by comma.
        // Since the number of lhs attribute indexes equals the number of lhs attribute names,
        // we can take the midpoint.
        String[] leftTokens = leftPart.split(",");
        int numTokens = leftTokens.length;

        // There should be an even number of tokens.
        if (numTokens % 2 != 0) {
            System.err.println("Problem");
            return null;
        }
        int n = numTokens / 2;

        String[] lhsAttrTokens = Arrays.copyOfRange(leftTokens, 0, n);
        String[] lhsNameTokens = Arrays.copyOfRange(leftTokens, n, numTokens);

        // Map each lhs attribute index to its name.
        for (int i = 0; i < n; i++) {
            try {
                int attrIndex = Integer.parseInt(lhsAttrTokens[i].trim());
                String attrName = lhsNameTokens[i].trim();
                indexToName.put(attrIndex, attrName);
            } catch (NumberFormatException e) {
                // Handle unexpected token format
                e.printStackTrace();
                return null;
            }
        }

        // Process the right part: should contain two tokens ("rhsAttr,rhsName")
        String[] rightTokens = rightPart.split(",");
        if (rightTokens.length < 2) {
            System.err.println("Problem");
            return null;
        }
        try {
            int rhsAttr = Integer.parseInt(rightTokens[0].trim());
            String rhsName = rightTokens[1].trim();
            indexToName.put(rhsAttr, rhsName);

            // For display, join the lhs names with a semicolon, then append "->" and the rhsName.
            String lhsNamesCombined = String.join(", ",
                    Arrays.stream(lhsNameTokens).map(String::trim).collect(Collectors.toList()));
            return lhsNamesCombined + "->" + rhsName;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return null;
        }
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


    public static List<Result> executeHyFD(double value, String... names) {
        List<Result> allResults = new ArrayList<>();
        try {
            for (String fileName : names) {
                RelationalInputGenerator input = getInputGenerator(fileName);
                ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(input));

                HyFD hyFD = createHyFD(value, input, resultReceiver);

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

    public static HyFD createHyFD(double value, RelationalInputGenerator input, ResultCache resultReceiver) throws AlgorithmConfigurationException {
        HyFD hyFD = new HyFD();
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
        hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), ""+value);//96,80
        hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), -1);
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


