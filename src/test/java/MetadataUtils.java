import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithm_integration.results.Result;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MetadataUtils {

    public static List<Pair<RelaxedFunctionalDependency, PdepTuple>> getPdeps(List<Result> fds, File[] fileNames) {
        Map<String, List<String>> columnData;
        try {
            columnData = getDataMap(fileNames);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Pair<RelaxedFunctionalDependency, PdepTuple>> results = new ArrayList<>();
        for (Result fd : fds) {
            RelaxedFunctionalDependency rfd = (RelaxedFunctionalDependency) fd;
            if (rfd.getDeterminant().getColumnIdentifiers().isEmpty()){
                for (String columnName : columnData.keySet()){
                    if (columnName.equals(rfd.getDependant().getColumnIdentifier()))
                        continue;
                    ColumnIdentifier ci = new ColumnIdentifier(fileNames[0].getName(),columnName);
                    RelaxedFunctionalDependency newrfd = new RelaxedFunctionalDependency(new ColumnCombination(ci),rfd.getDependant(), rfd.getMeasure());
                    PdepTuple pdep = getPdep(newrfd, columnData);
                    Pair<RelaxedFunctionalDependency, PdepTuple> pair = new Pair<>(newrfd, pdep);
                    if (!results.contains(pair))
                        results.add(pair);
                    else
                        System.out.println("Already contains " + newrfd);
                }
            } else {
                PdepTuple pdep = getPdep(rfd, columnData);
                results.add(new Pair<>(rfd, pdep));
            }
        }
        return results;
    }

    private static Map<String, List<String>> getDataMap(File[] fileNames) throws IOException {
        Map<String, List<String>> columnData = new HashMap<>();

        for (File csvFile : fileNames) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
                String headerLine = br.readLine();
                if (headerLine == null) continue;

                String[] headers = headerLine.split(",");
                List<List<String>> columns = new ArrayList<>();

                // Initialize list for each column
                for (int i = 0; i < headers.length; i++) {
                    columns.add(new ArrayList<>());
                }

                String line;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",", -1); // -1 keeps empty strings
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        columns.get(i).add(values[i]);
                    }
                }

                // Add to global map (column name -> list of values)
                for (int i = 0; i < headers.length; i++) {
                    String columnName = headers[i];
                    columnData.computeIfAbsent(columnName, k -> new ArrayList<>())
                            .addAll(columns.get(i));
                }
            }
        }
        return columnData;
    }

    public static PdepTuple getPdep(RelaxedFunctionalDependency fd, Map<String, List<String>> columnData) {
        ColumnCombination determinant = fd.getDeterminant();
        ColumnIdentifier dependant = fd.getDependant();
        int N = columnData.get(dependant.getColumnIdentifier()).size();
        Map<String, Integer> frequencyMapDep = createFrequencyMap(dependant, columnData);
        Map<String, Integer> frequencyMapDet = createFrequencyMap(determinant, N, columnData);

        double pdep = fd.getMeasure();
        double pdep2 = 1d;

        if (!determinant.getColumnIdentifiers().isEmpty()){
            pdep2 = pdep(determinant.getColumnIdentifiers(), dependant.getColumnIdentifier(), columnData);
        }
        double gpdep = gpdep(frequencyMapDet, frequencyMapDep, N, pdep2);

        return new PdepTuple(pdep2, gpdep);
    }

    public static double epdep(int dA, Map<String, Integer> valuesB, int N) {
        double pdepB = pdep(valuesB, N);
        return pdepB + (dA - 1.0) / (N - 1.0) * (1.0 - pdepB);
    }

    private static double pdep(Set<ColumnIdentifier> determine, String dependent, Map<String, List<String>> columnData) {
        int numRecords = columnData.get(new ArrayList<>(determine).get(0).getColumnIdentifier().toString()).size();
        List<String> detValues = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            StringBuilder compositeValue = new StringBuilder();
            // For each ColumnIdentifier in our ordered list, append its value at record i.
            for (ColumnIdentifier cid : determine) {
                // Assuming the key is obtained by toString(); if you have another method, use that.
                String colName = cid.getColumnIdentifier().toString();
                List<String> colData = columnData.get(colName);
                if (colData == null) {
                    // If a column is missing, append a marker.
                    compositeValue.append("null");
                } else {
                    compositeValue.append(colData.get(i));
                }
            }
            detValues.add(compositeValue.toString());
        }
        List<String> depValues = columnData.get(dependent);

        if (detValues == null || depValues == null || detValues.size() != depValues.size()) {
            throw new IllegalArgumentException("Invalid input columns or mismatched sizes.");
        }

        int N = detValues.size();

        // Group dependent values by determinant values
        Map<String, Map<String, Integer>> groupedCounts = new HashMap<>();

        for (int i = 0; i < N; i++) {
            String detVal = detValues.get(i);
            String depVal = depValues.get(i);

            groupedCounts
                    .computeIfAbsent(detVal, k -> new HashMap<>())
                    .merge(depVal, 1, Integer::sum);
        }

        // For each group, find the max count of the most frequent dependent value
        int sumMaxCounts = 0;
        for (Map<String, Integer> depCounts : groupedCounts.values()) {
            int maxCount = Collections.max(depCounts.values());
            sumMaxCounts += maxCount;
        }

        return (double) sumMaxCounts / N;
    }

    private static double pdep(Map<String, Integer> valuesB, int N) {
        double result = 0;
        for (Integer count : valuesB.values()) {
            result += (count * count);
        }
        return result / (N * N);
    }

    public static double gpdep(Map<String, Integer> valuesA, Map<String, Integer> valuesB, int N, double fdMeasure) {
        double pdepAB = fdMeasure;
        double epdepAB = epdep(valuesA.size(), valuesB, N);
        return pdepAB - epdepAB;
    }

    public static Map<String, Integer> createFrequencyMap(ColumnIdentifier column, Map<String, List<String>> columnData) {
        Map<String, Integer> frequencyMap = new HashMap<>();
        for (String value : columnData.get(column.getColumnIdentifier())) {
            frequencyMap.put(value, frequencyMap.getOrDefault(value, 0) + 1);
        }
        return frequencyMap;
    }

    public static Map<String, Integer> createFrequencyMap(ColumnCombination columns, int size, Map<String, List<String>> columnData) {
        Map<String, Integer> frequencyMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            StringBuilder concatenatedValue = new StringBuilder();
            for (ColumnIdentifier col : columns.getColumnIdentifiers()) {
                concatenatedValue.append(columnData.get(col.getColumnIdentifier()).get(i));
            }
            String key = concatenatedValue.toString();
            frequencyMap.put(key, frequencyMap.getOrDefault(key, 0) + 1);
        }
        return frequencyMap;
    }
}
