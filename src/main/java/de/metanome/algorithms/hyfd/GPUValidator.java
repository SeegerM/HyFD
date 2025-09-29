package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * This class orchestrates the validation of multiple Functional Dependencies on the GPU.
 */
public class GPUValidator {

    private TornadoExecutionPlan executionPlan;
    private boolean isPlanCreated = false;

    public static void validateClustersKernelOptimized(
            final int[] recordsFlat,
            final int nRows,
            final int nCols,
            final int[] clusterData,
            final int[] clusterStart,
            final int[] clusterLen,
            final int[] rhsAttrs,
            final int[] outViolations,
            final int maxValueRange) {

        for (@Parallel int c = 0; c < clusterLen.length; c++) {
            int start = clusterStart[c];
            int len = clusterLen[c];
            int rhs = rhsAttrs[c];

            if (len <= 1) {
                outViolations[c] = 0;
                continue;
            }

            int mode = -1;
            int maxCount = 0;

            // Small value range: Use a direct histogram (efficient)
            if (maxValueRange <= 1024) { // A slightly larger threshold might be beneficial
                int[] histogram = new int[maxValueRange];

                // Pass 1: Build the histogram
                for (int i = 0; i < len; i++) {
                    int rid = clusterData[start + i];
                    int val = recordsFlat[rid * nCols + rhs];
                    if (val >= 0 && val < maxValueRange) {
                        histogram[val]++;
                    }
                }

                // Find the mode from the histogram
                for (int i = 0; i < maxValueRange; i++) {
                    if (histogram[i] > maxCount) {
                        maxCount = histogram[i];
                        mode = i;
                    }
                }
            } else {
                // Large value range: Two-pass approach to find the mode
                // Pass 1: Find the mode. This is still O(n^2) in its simplest form,
                // but often faster on a GPU than complex data structures.
                // The old kernel's mode finding is a good example.
                for (int i = 0; i < len; i++) {
                    int currentRid = clusterData[start + i];
                    int currentValue = recordsFlat[currentRid * nCols + rhs];
                    if (currentValue == -1) continue;

                    int currentCount = 0;
                    for (int j = 0; j < len; j++) {
                        int compareRid = clusterData[start + j];
                        if (recordsFlat[compareRid * nCols + rhs] == currentValue) {
                            currentCount++;
                        }
                    }

                    if (currentCount > maxCount) {
                        maxCount = currentCount;
                        mode = currentValue;
                    }
                }
            }

            // Pass 2: Count violations against the determined mode
            if (mode == -1) {
                // If all values were invalid or the cluster was empty after filtering
                outViolations[c] = len > 0 ? len - 1 : 0;
            } else {
                int violations = len - maxCount;
                outViolations[c] = violations;
            }
        }
    }

    public static void validateClustersKernelOptimizedOld(
            final int[] recordsFlat,
            final int nRows,
            final int nCols,
            final int[] clusterData,
            final int[] clusterStart,
            final int[] clusterLen,
            final int[] rhsAttrs,
            final int[] outViolations,
            final int maxValueRange) {

        for (@Parallel int c = 0; c < clusterLen.length; c++) {
            int start = clusterStart[c];
            int len = clusterLen[c];
            int rhs = rhsAttrs[c];

            if (len <= 1) {
                outViolations[c] = 0;
                continue;
            }

            // Use local histogram for small value ranges
            // For larger ranges, use a two-pass approach
            if (maxValueRange <= 1000) {
                // Small range: use direct histogram
                int[] histogram = new int[maxValueRange];
                int maxCount = 0;
                int mode = -1;

                // Build histogram
                for (int i = 0; i < len; i++) {
                    int rid = clusterData[start + i];
                    if (rid >= 0 && rid < nRows) {
                        int val = recordsFlat[rid * nCols + rhs];
                        if (val >= 0 && val < maxValueRange) {
                            histogram[val]++;
                            if (histogram[val] > maxCount) {
                                maxCount = histogram[val];
                                mode = val;
                            }
                        }
                    }
                }

                // Count violations
                if (mode == -1) {
                    outViolations[c] = len - 1; // All but one are violations
                } else {
                    int violations = 0;
                    for (int i = 0; i < len; i++) {
                        int rid = clusterData[start + i];
                        if (rid >= 0 && rid < nRows) {
                            if (recordsFlat[rid * nCols + rhs] != mode) {
                                violations++;
                            }
                        }
                    }
                    outViolations[c] = violations;
                }
            } else {
                // Large range: use sampling approach for mode estimation
                // Sample first few elements to find candidate mode
                int sampleSize = Math.min(10, len);
                int bestCandidate = -1;
                int bestCount = 0;

                for (int i = 0; i < sampleSize; i++) {
                    int rid = clusterData[start + i];
                    if (rid >= 0 && rid < nRows) {
                        int candidate = recordsFlat[rid * nCols + rhs];
                        if (candidate != -1) {
                            int count = 0;
                            // Count occurrences of this candidate
                            for (int j = 0; j < len; j++) {
                                int rid2 = clusterData[start + j];
                                if (rid2 >= 0 && rid2 < nRows &&
                                        recordsFlat[rid2 * nCols + rhs] == candidate) {
                                    count++;
                                }
                            }
                            if (count > bestCount) {
                                bestCount = count;
                                bestCandidate = candidate;
                            }
                        }
                    }
                }

                // Count violations based on best candidate
                if (bestCandidate == -1) {
                    outViolations[c] = len - 1;
                } else {
                    int violations = 0;
                    for (int i = 0; i < len; i++) {
                        int rid = clusterData[start + i];
                        if (rid >= 0 && rid < nRows) {
                            if (recordsFlat[rid * nCols + rhs] != bestCandidate) {
                                violations++;
                            }
                        }
                    }
                    outViolations[c] = violations;
                }
            }
        }
    }

    /**
     * Optimized mass validation that minimizes memory transfers
     */
    public void performMassValidation(List<ValidationTask> validationTasks,
                                      int[][] compressedRecords,
                                      int maxViolations) {
        if (validationTasks.isEmpty()) return;

        int nRows = compressedRecords.length;
        int nCols = compressedRecords[0].length;

        // Flattening the records is necessary for GPU transfer
        int[] recordsFlat = flattenRecords(compressedRecords, nRows, nCols);

        // Pre-calculate the total size to avoid list resizing
        int totalClusterEntries = 0;
        int totalClusters = 0;
        for (ValidationTask task : validationTasks) {
            for (IntArrayList cluster : task.getLhsPli().getClusters()) {
                totalClusterEntries += cluster.size();
                totalClusters++;
            }
        }

        // Use pre-sized IntArrayList from fastutil for better performance
        IntArrayList clusterDataList = new IntArrayList(totalClusterEntries);
        IntArrayList clusterStartList = new IntArrayList(totalClusters);
        IntArrayList clusterLenList = new IntArrayList(totalClusters);
        IntArrayList rhsList = new IntArrayList(totalClusters);

        int[] taskStart = new int[validationTasks.size() + 1];
        int taskIdx = 0;
        int currentClusterIndex = 0;

        for (ValidationTask task : validationTasks) {
            taskStart[taskIdx] = currentClusterIndex;
            PositionListIndex pli = task.getLhsPli();
            int rhs = task.getRhsAttr();

            for (IntArrayList cluster : pli.getClusters()) {
                clusterStartList.add(clusterDataList.size());
                clusterLenList.add(cluster.size());
                clusterDataList.addElements(clusterDataList.size(), cluster.elements(), 0, cluster.size());
                rhsList.add(rhs);
                currentClusterIndex++;
            }
            taskIdx++;
        }
        taskStart[taskIdx] = totalClusters;

        // Convert to arrays
        int[] clusterData = clusterDataList.toIntArray();
        int[] clusterStart = clusterStartList.toIntArray();
        int[] clusterLen = clusterLenList.toIntArray();
        int[] rhsAttrs = rhsList.toIntArray();
        int[] outViolations = new int[clusterLen.length];

        // Determine max value range for optimization
        Set<Integer> neededColumns = new HashSet<>();
        for (ValidationTask task : validationTasks) {
            neededColumns.add(task.getRhsAttr());
        }
        int maxValueRange = findMaxValueRange(compressedRecords, neededColumns);

        // Execute on GPU
        executeOnGPU(recordsFlat, nRows, nCols, clusterData, clusterStart,
                clusterLen, rhsAttrs, outViolations, maxValueRange);

        // Process results
        for (int i = 0; i < validationTasks.size(); i++) {
            int startIdx = taskStart[i];
            int endIdx = taskStart[i + 1];
            int totalViolations = 0;

            for (int j = startIdx; j < endIdx; j++) {
                totalViolations += outViolations[j];
            }

            ValidationTask task = validationTasks.get(i);
            task.setViolations(totalViolations);
            task.setIsValid(totalViolations <= maxViolations);
        }
    }

    public void performMassValidationOld2(List<ValidationTask> validationTasks,
                                          int[][] compressedRecords,
                                          int maxViolations) {
        if (validationTasks.isEmpty()) return;

        int nRows = compressedRecords.length;
        int nCols = compressedRecords[0].length;

        // Determine which columns are actually needed
        Set<Integer> neededColumns = new HashSet<>();
        for (ValidationTask task : validationTasks) {
            neededColumns.add(task.getRhsAttr());
            // Add LHS columns if we had them
        }

        // Option 1: Transfer only needed columns (more complex but efficient)
        // Option 2: Transfer all (simpler, what we'll use here)
        int[] recordsFlat = flattenRecords(compressedRecords, nRows, nCols);

        // Build cluster arrays
        List<Integer> clusterDataList = new ArrayList<>();
        List<Integer> clusterStartList = new ArrayList<>();
        List<Integer> clusterLenList = new ArrayList<>();
        List<Integer> rhsList = new ArrayList<>();

        int[] taskStart = new int[validationTasks.size() + 1];
        int taskIdx = 0;

        for (ValidationTask task : validationTasks) {
            taskStart[taskIdx] = clusterLenList.size();
            PositionListIndex pli = task.getLhsPli();
            int rhs = task.getRhsAttr();

            for (IntArrayList cluster : pli.getClusters()) {
                int start = clusterDataList.size();
                clusterDataList.addAll(cluster);
                clusterStartList.add(start);
                clusterLenList.add(cluster.size());
                rhsList.add(rhs);
            }
            taskIdx++;
        }
        taskStart[taskIdx] = clusterLenList.size();

        // Convert to arrays
        int[] clusterData = clusterDataList.stream().mapToInt(Integer::intValue).toArray();
        int[] clusterStart = clusterStartList.stream().mapToInt(Integer::intValue).toArray();
        int[] clusterLen = clusterLenList.stream().mapToInt(Integer::intValue).toArray();
        int[] rhsAttrs = rhsList.stream().mapToInt(Integer::intValue).toArray();
        int[] outViolations = new int[clusterLen.length];

        // Determine max value range for optimization
        int maxValueRange = findMaxValueRange(compressedRecords, neededColumns);

        // Execute on GPU with optimized kernel
        executeOnGPU(recordsFlat, nRows, nCols, clusterData, clusterStart,
                clusterLen, rhsAttrs, outViolations, maxValueRange);

        // Process results
        for (int i = 0; i < validationTasks.size(); i++) {
            int startIdx = taskStart[i];
            int endIdx = taskStart[i + 1];
            int totalViolations = 0;

            for (int j = startIdx; j < endIdx; j++) {
                totalViolations += outViolations[j];
            }

            ValidationTask task = validationTasks.get(i);
            task.setViolations(totalViolations);
            task.setIsValid(totalViolations <= maxViolations);
        }
    }

    private void executeOnGPU(int[] recordsFlat, int nRows, int nCols,
                              int[] clusterData, int[] clusterStart, int[] clusterLen,
                              int[] rhsAttrs, int[] outViolations, int maxValueRange) {

        if (!isPlanCreated) {
            // Create execution plan once and reuse
            TaskGraph tg = new TaskGraph("fdValidation")
                    .transferToDevice(DataTransferMode.EVERY_EXECUTION,
                            recordsFlat, clusterData, clusterStart, clusterLen, rhsAttrs)
                    .task("validate", GPUValidator::validateClustersKernelOptimized,
                            recordsFlat, nRows, nCols, clusterData, clusterStart,
                            clusterLen, rhsAttrs, outViolations, maxValueRange)
                    .transferToHost(DataTransferMode.EVERY_EXECUTION, outViolations);

            executionPlan = new TornadoExecutionPlan(tg.snapshot());
            isPlanCreated = false;
        }

        executionPlan.execute();
    }

    private int[] flattenRecords(int[][] compressedRecords, int nRows, int nCols) {
        int[] flat = new int[nRows * nCols];
        for (int r = 0; r < nRows; r++) {
            System.arraycopy(compressedRecords[r], 0, flat, r * nCols, nCols);
        }
        return flat;
    }

    private int findMaxValueRange(int[][] records, Set<Integer> columns) {
        int max = 0;
        for (int[] row : records) {
            for (int col : columns) {
                if (col < row.length && row[col] > max) {
                    max = row[col];
                }
            }
        }
        return max + 1; // Range is 0 to max inclusive
    }

    /**
     * GPU KERNEL: This method represents the code that will run on the GPU.
     * It's written in Java, and TornadoVM will compile it to OpenCL/CUDA.
     * It calculates the number of violations for each cluster of a given LHS PLI against a specific RHS attribute.
     */
    public static void validateClustersKernelOld(
            final int[] recordsFlat,   // row-major: r*nCols + c
            final int nRows,
            final int nCols,
            final int[] clusterData,   // record IDs, concatenated
            final int[] clusterStart,  // start index in clusterData
            final int[] clusterLen,    // length per cluster
            final int[] rhsAttrs,      // RHS attribute per cluster
            final int[] outViolations) {

        for (@Parallel int c = 0; c < clusterLen.length; c++) {
            int start = clusterStart[c];
            int len   = clusterLen[c];
            int rhs   = rhsAttrs[c];

            if (len <= 1) {
                outViolations[c] = 0;
                continue;
            }

            // --- pass 1: find dominant RHS value (mode) ---
            // Simple O(n^2) mode for clarity; for big clusters you can
            // replace with a small fixed-size hashmap or counting pass.
            int domVal = -1, domCnt = 0;
            for (int i = 0; i < len; i++) {
                int rid = clusterData[start + i];
                int v   = recordsFlat[rid * nCols + rhs];
                if (v == -1) continue;
                int cnt = 0;
                for (int j = 0; j < len; j++) {
                    int rid2 = clusterData[start + j];
                    if (recordsFlat[rid2 * nCols + rhs] == v) cnt++;
                }
                if (cnt > domCnt) { domCnt = cnt; domVal = v; }
            }

            if (domVal == -1) { // all missing → size-1 per CPU logic
                outViolations[c] = Math.max(0, len - 1);
                continue;
            }

            // --- pass 2: count mismatches to mode ---
            int viol = 0;
            for (int i = 0; i < len; i++) {
                int rid = clusterData[start + i];
                if (recordsFlat[rid * nCols + rhs] != domVal) viol++;
            }
            outViolations[c] = viol;
        }
    }

    /**
     * HOST METHOD: This is the main method called from the CPU.
     * It prepares data from many FDs, sends it to the GPU, runs the kernel, and processes the results.
     *
     * @param validationTasks   A list of tasks, each representing an LHS -> RHS validation.
     * @param compressedRecords The full dataset.
     * @param maxViolations     The maximum allowed violations per FD.
     */
    public void performMassValidationOld(List<ValidationTask> validationTasks, int[][] compressedRecords, int maxViolations) {
        int nRows = compressedRecords.length;
        int nCols = compressedRecords[0].length;
        int[] recordsFlat = new int[nRows * nCols];
        for (int r = 0; r < nRows; r++) {
            int base = r * nCols;
            System.arraycopy(compressedRecords[r], 0, recordsFlat, base, nCols);
        }

// 2) Build cluster arrays without boxing
        it.unimi.dsi.fastutil.ints.IntArrayList clusterDataAL = new it.unimi.dsi.fastutil.ints.IntArrayList();
        it.unimi.dsi.fastutil.ints.IntArrayList clusterStartAL = new it.unimi.dsi.fastutil.ints.IntArrayList();
        it.unimi.dsi.fastutil.ints.IntArrayList clusterLenAL   = new it.unimi.dsi.fastutil.ints.IntArrayList();
        it.unimi.dsi.fastutil.ints.IntArrayList rhsAL          = new it.unimi.dsi.fastutil.ints.IntArrayList();
        it.unimi.dsi.fastutil.ints.IntArrayList taskStartAL    = new it.unimi.dsi.fastutil.ints.IntArrayList();

        // map task → [startCluster, endCluster)
        int[] taskStart = new int[validationTasks.size() + 1];
        int tIdx = 0;

        for (ValidationTask t : validationTasks) {
            taskStart[tIdx] = clusterLenAL.size();
            PositionListIndex pli = t.getLhsPli();
            int rhs = t.getRhsAttr();

            for (it.unimi.dsi.fastutil.ints.IntArrayList cluster : pli.getClusters()) {
                int start = clusterDataAL.size();
                int len   = cluster.size();
                for (int i = 0; i < len; i++) clusterDataAL.add(cluster.getInt(i));
                clusterStartAL.add(start);
                clusterLenAL.add(len);
                rhsAL.add(rhs);
            }
            tIdx++;
        }
        taskStart[tIdx] = clusterLenAL.size(); // sentinel


        int[] clusterData = clusterDataAL.toIntArray();
        int[] clusterStart = clusterStartAL.toIntArray();
        int[] clusterLen   = clusterLenAL.toIntArray();
        int[] rhsAttrs     = rhsAL.toIntArray();
        int[] out          = new int[clusterLen.length];

// (Optional) sanity for huge transfers
        long bytesRecords = 1L * nRows * nCols * 4L;
        long bytesClusters = 1L * clusterData.length * 4L;
        if (bytesRecords > Integer.MAX_VALUE || bytesClusters > Integer.MAX_VALUE) {
            // TODO: chunk the work to keep copies < 2^31-1 bytes
            System.out.println("TODO FIX");
        }

// 3) Task graph
        TaskGraph tg = new TaskGraph("s0")
                .transferToDevice(DataTransferMode.FIRST_EXECUTION,
                        recordsFlat, clusterData, clusterStart, clusterLen, rhsAttrs)
                .task("t0", GPUValidator::validateClustersKernelOld,
                        recordsFlat, nRows, nCols, clusterData, clusterStart, clusterLen, rhsAttrs, out)
                .transferToHost(DataTransferMode.EVERY_EXECUTION, out);

        new TornadoExecutionPlan(tg.snapshot()).execute();

// 4) Aggregate back per task
        for (int i = 0; i < validationTasks.size(); i++) {
            int s = taskStart[i];
            int e = (i + 1 < taskStart.length) ? taskStart[i + 1] : out.length;
            int total = 0;
            for (int j = s; j < e; j++) total += out[j];
            ValidationTask t = validationTasks.get(i);
            t.setViolations(total);
            t.setIsValid(total <= maxViolations);
        }

    }

    // Helper class to represent a single validation task
    public static class ValidationTask {
        private final PositionListIndex lhsPli;
        private final int rhsAttr;
        private int violations;
        private boolean isValid;

        public ValidationTask(PositionListIndex lhsPli, int rhsAttr) {
            this.lhsPli = lhsPli;
            this.rhsAttr = rhsAttr;
        }

        // Getters and Setters
        public PositionListIndex getLhsPli() { return lhsPli; }
        public int getRhsAttr() { return rhsAttr; }
        public int getViolations() { return violations; }
        public boolean isValid() { return isValid; }
        public void setViolations(int violations) { this.violations = violations; }
        public void setIsValid(boolean isValid) { this.isValid = isValid; }

        @Override
        public String toString() {
            return "LHS(attr=" + lhsPli.attribute + ") -> RHS(attr=" + rhsAttr + ")";
        }
    }
}