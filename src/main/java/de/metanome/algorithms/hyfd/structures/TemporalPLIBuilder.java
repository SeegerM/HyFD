package de.metanome.algorithms.hyfd.structures;

import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithms.hyfd.utils.InMemoryTemporalTableGenerator;
import de.metanome.algorithms.hyfd.utils.ValueComparator;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TemporalPLIBuilder {

    // A simple container to return all the structures at once
    public static class TemporalData {
        public final List<PositionListIndex> plis;
        public final int[][] compressedRecords;
        public final long[] timestamps;
        public final int numRecords;

        public TemporalData(List<PositionListIndex> plis, int[][] compressedRecords, long[] timestamps, int numRecords) {
            this.plis = plis;
            this.compressedRecords = compressedRecords;
            this.timestamps = timestamps;
            this.numRecords = numRecords;
        }
    }

    private final List<Map<String, Integer>> valueMaps;
    private final List<PositionListIndex> plis;
    private final List<List<String>> records;
    private final SimpleDateFormat timestampParser;

    private final int numAttributes;

    public TemporalPLIBuilder(int numAttributes) {
        this.numAttributes = numAttributes;
        this.valueMaps = new ArrayList<>(numAttributes);
        this.plis = new ArrayList<>(numAttributes);
        this.records = new ArrayList<>();

        // Define the format of your timestamp string. This must match the input file.
        // Example: "Sat Sep 07 02:54:27 CEST 2013"
        this.timestampParser = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

        for (int i = 0; i < numAttributes; i++) {
            this.valueMaps.add(new HashMap<>());
            this.plis.add(new PositionListIndex(i, new ArrayList<>()));
        }
    }

    public TemporalData build(RelationalInput relationalInput) throws InputIterationException {
        int recordId = 0;

        // Build compressed rows and timestamps as we stream the input once
        List<int[]> compressedRows = new ArrayList<>();
        List<Long> timestampsList = new ArrayList<>();

        while (relationalInput.hasNext()) {
            List<String> row = relationalInput.next();

            // Parse timestamp (last column)
            String timestampStr = row.get(this.numAttributes-1);
            long ts;
            try {
                ts = timestampParser.parse(timestampStr).getTime();
            } catch (ParseException e) {
                ts = 0L; // or whatever default you prefer
                System.err.println("Could not parse timestamp: " + timestampStr);
            }
            timestampsList.add(ts);

            // Build compressed codes for this row while updating PLIs and value maps
            int[] codes = new int[this.numAttributes];
            for (int i = 0; i < this.numAttributes; i++) {
                Map<String, Integer> valueMap = this.valueMaps.get(i);
                String value = row.get(i);

                Integer idx = valueMap.get(value);
                if (idx == null) {
                    // New value → create cluster and remember its index
                    plis.get(i).getClusters().add(new it.unimi.dsi.fastutil.ints.IntArrayList());
                    idx = plis.get(i).getClusters().size() - 1; // index of the newly added cluster
                    valueMap.put(value, idx);
                }

                // Add this record to the appropriate cluster and to the compressed row
                plis.get(i).getClusters().get(idx).add(recordId);
                codes[i] = idx;
            }

            compressedRows.add(codes);
            recordId++;
        }

        int numRecords = recordId;
        if (numRecords == 0) {
            return new TemporalData(new ArrayList<>(), new int[0][0], new long[0], 0);
        }

        // Convert lists to arrays expected by TemporalData
        int[][] compressedRecords = compressedRows.toArray(new int[numRecords][]);

        long[] timestamps = new long[numRecords];
        for (int i = 0; i < numRecords; i++) timestamps[i] = timestampsList.get(i);

        return new TemporalData(plis, compressedRecords, timestamps, numRecords);
    }

    public TemporalData buildOld(RelationalInput relationalInput) throws InputIterationException {
        // First pass: Read all data, build value maps, and store records
        int recordId = 0;
        while (relationalInput.hasNext()) {
            List<String> row = relationalInput.next();

            // The last column is the timestamp
            List<String> dataValues = row.subList(0, this.numAttributes);
            records.add(dataValues);

            for (int i = 0; i < this.numAttributes; i++) {
                Map<String, Integer> valueMap = this.valueMaps.get(i);
                String value = dataValues.get(i);

                if (!valueMap.containsKey(value)) {
                    // Add a new cluster for a new value
                    plis.get(i).getClusters().add(new IntArrayList());
                    valueMap.put(value, (int) (plis.get(i).size() - 1));
                }
                // Add the recordId to the corresponding cluster
                plis.get(i).getClusters().get(valueMap.get(value)).add(recordId);
            }
            recordId++;
        }

        int numRecords = records.size();
        if (numRecords == 0) {
            return new TemporalData(new ArrayList<>(), new int[0][0], new long[0], 0);
        }

        // Second pass: Create compressed records and timestamps array
        int[][] compressedRecords = new int[numRecords][this.numAttributes];
        long[] timestamps = new long[numRecords];

        // We need to re-open the input to get timestamps in the correct order
        // This is necessary because the iterator is now exhausted.
        //relationalInput = ((InMemoryTemporalTableGenerator) relationalInput.getGenerator()).generateNewCopy();

        recordId = 0;
        while (relationalInput.hasNext()) {
            List<String> row = relationalInput.next();
            String timestampStr = row.get(this.numAttributes);
            try {
                timestamps[recordId] = timestampParser.parse(timestampStr).getTime();
            } catch (ParseException e) {
                // Handle error or set a default timestamp
                timestamps[recordId] = 0;
                System.err.println("Could not parse timestamp: " + timestampStr);
            }

            for (int i = 0; i < this.numAttributes; i++) {
                String value = records.get(recordId).get(i);
                compressedRecords[recordId][i] = this.valueMaps.get(i).get(value);
            }
            recordId++;
        }

        return new TemporalData(plis, compressedRecords, timestamps, numRecords);
    }

    public int getNumLastRecords() {
        return records.size();
    }
}