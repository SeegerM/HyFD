package de.metanome.algorithms.hyfd;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.utils.MetadataUtils;
import de.metanome.algorithms.hyfd.utils.Pair;
import de.metanome.algorithms.hyfd.utils.PdepTuple;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Runner {

    static String path = "../../../../../data/hospital/clean/";
    public static void main(String[] args) {
        File csvFile = new File("../../../../../data/hospital/clean/clean.csv");

        if (!csvFile.exists()) {
            throw new RuntimeException("CSV file does not exist: " + csvFile.getAbsolutePath());
        }

        Runner.path = csvFile.getParent();

        System.out.println("Running HyFD on: " + csvFile.getAbsolutePath());

        double threshold = 0.95d;

        long startTime = System.nanoTime();
        HyFD.Mode mode = HyFD.Mode.GPDEP;

        List<Result> hyfdResults = executeHyFD(mode, threshold, csvFile.getName());

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
        return executeHyFD(HyFD.Mode.STANDARD, value, names);
    }

    public static List<Result> executeHyFD(HyFD.Mode mode, double value, String... names) {
        List<Result> allResults = new ArrayList<>();
        try {
            for (String fileName : names) {
                RelationalInputGenerator input = getInputGenerator(fileName);
                ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(input));

                HyFD hyFD = createHyFD(value, input, resultReceiver);
                hyFD.setMode(mode);

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


