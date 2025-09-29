package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import it.unimi.dsi.fastutil.ints.*;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Mass FD validator with GPU acceleration for small RHS domains and CPU fallback for large ones.
 */
public class GPUValidatorReusePlan {

    // -------- persistent GPU plan & buffers (to avoid re-transfers) ----------
    private TornadoExecutionPlan plan;
    private boolean planBuilt = false;

    // Persistent host-side arrays that the plan points to
    private int[] d_recordsFlat;
    private int[] d_clusterDataSmall;
    private int[] d_clusterStartSmall;
    private int[] d_clusterLenSmall;
    private int[] d_rhsAttrsSmall;
    private int[] d_outViolationsSmall;
    private int[] d_histogramBuffer; // size = numSmallClusters * maxSmallRange

    // Current capacities to know when to (re)allocate
    private int capRecords = 0;
    private int capClusterDataSmall = 0;
    private int capClustersSmall = 0;
    private int capHistogram = 0;

    // Threshold for “small range” (histogram path). Tune as needed.
    private static final int SMALL_RANGE_THRESHOLD = 1024;

    // ------------------------ PUBLIC API -------------------------------------

    /**
     * Validates many LHS->RHS tasks. GPU handles clusters whose RHS range <= threshold.
     * CPU handles the rest (exact, O(n) counting via int map).
     */
    public void performMassValidation(List<GPUValidator.ValidationTask> validationTasks,
                                      int[][] compressedRecords,
                                      int maxViolations) {
        if (validationTasks.isEmpty()) return;

        final int nRows = compressedRecords.length;
        final int nCols = compressedRecords[0].length;

        // Flatten once
        int[] recordsFlat = flattenRecords(compressedRecords, nRows, nCols);

        // Figure out which RHS columns we need, then compute max value per RHS
        Set<Integer> neededRhs = new HashSet<>();
        for (GPUValidator.ValidationTask t : validationTasks) neededRhs.add(t.getRhsAttr());
        int[] rhsMaxPlus1 = computeColumnRanges(compressedRecords, neededRhs, nCols);

        // Build cluster arrays; partition into small-range (GPU) and large-range (CPU)
        // We also keep task -> [start,end) mapping for both partitions.
        PartitionedClusters part = buildAndPartitionClusters(validationTasks, rhsMaxPlus1);

        // --- GPU for small-range clusters ---
        if (part.numSmallClusters > 0) {
            int maxSmallRange = part.maxSmallRange; // <= SMALL_RANGE_THRESHOLD
            ensureGpuBuffers(recordsFlat.length,
                    part.clusterDataSmall.size(),
                    part.numSmallClusters,
                    maxSmallRange);

            // Copy host → persistent host buffers that the plan uses
            System.arraycopy(recordsFlat, 0, d_recordsFlat, 0, recordsFlat.length);
            System.arraycopy(part.clusterDataSmall.elements(), 0, d_clusterDataSmall, 0, part.clusterDataSmall.size());
            System.arraycopy(part.clusterStartSmall.elements(), 0, d_clusterStartSmall, 0, part.numSmallClusters);
            System.arraycopy(part.clusterLenSmall.elements(), 0, d_clusterLenSmall, 0, part.numSmallClusters);
            System.arraycopy(part.rhsAttrsSmall.elements(), 0, d_rhsAttrsSmall, 0, part.numSmallClusters);

            // Build / rebuild the plan if needed
            if (!planBuilt) {
                TaskGraph tg = new TaskGraph("fdValidationSmall")
                        .transferToDevice(DataTransferMode.FIRST_EXECUTION,
                                d_recordsFlat, d_clusterDataSmall, d_clusterStartSmall, d_clusterLenSmall, d_rhsAttrsSmall,
                                d_histogramBuffer, d_outViolationsSmall)
                        .task("validateSmall", GPUValidatorReusePlan::validateClustersKernelSmallRange,
                                d_recordsFlat, nRows, nCols,
                                d_clusterDataSmall, d_clusterStartSmall, d_clusterLenSmall,
                                d_rhsAttrsSmall, d_outViolationsSmall,
                                part.numSmallClusters, maxSmallRange, d_histogramBuffer)
                        .transferToHost(DataTransferMode.EVERY_EXECUTION, d_outViolationsSmall);

                plan = new TornadoExecutionPlan(tg.snapshot());
                planBuilt = true;
            }

            // Execute
            plan.execute();

            // Copy GPU results back into the partition structure to aggregate per task
            part.outViolationsSmall.clear();
            part.outViolationsSmall.size(part.numSmallClusters);
            System.arraycopy(d_outViolationsSmall, 0, part.outViolationsSmall.elements(), 0, part.numSmallClusters);
        }

        // --- CPU for large-range clusters (exact counting; no O(n^2)) ---
        if (part.numLargeClusters > 0) {
            cpuValidateLargeRange(recordsFlat, nRows, nCols, part);
        }

        // --- Aggregate results per task ---
        for (int i = 0; i < validationTasks.size(); i++) {
            int total = 0;

            // Sum small partition violations for this task
            int ss = part.taskSmallStart[i], se = part.taskSmallStart[i + 1];
            for (int j = ss; j < se; j++) total += part.outViolationsSmall.getInt(j);

            // Sum large partition violations for this task
            int ls = part.taskLargeStart[i], le = part.taskLargeStart[i + 1];
            for (int j = ls; j < le; j++) total += part.outViolationsLarge.getInt(j);

            GPUValidator.ValidationTask t = validationTasks.get(i);
            t.setViolations(total);
            t.setIsValid(total <= maxViolations);
        }
    }

    // --------------------------- GPU KERNEL ----------------------------------

    /**
     * GPU kernel for small RHS value ranges (<= SMALL_RANGE_THRESHOLD).
     * Uses a single pre-allocated histogramBuffer laid out as
     * [numClusters][maxRange]. Each work-item zeroes & uses its slice.
     */
    public static void validateClustersKernelSmallRange(
            final int[] recordsFlat,
            final int nRows,
            final int nCols,
            final int[] clusterData,
            final int[] clusterStart,
            final int[] clusterLen,
            final int[] rhsAttrs,
            final int[] outViolations,
            final int numClusters,
            final int maxRange,
            final int[] histogramBuffer) {

        for (@Parallel int c = 0; c < numClusters; c++) {
            int start = clusterStart[c];
            int len   = clusterLen[c];
            int rhs   = rhsAttrs[c];

            if (len <= 1) {
                outViolations[c] = 0;
                continue;
            }

            // Slice for this cluster
            int base = c * maxRange;

            // Zero the histogram slice
            for (int k = 0; k < maxRange; k++) {
                histogramBuffer[base + k] = 0;
            }

            // Build histogram and track mode
            int maxCount = 0;
            int mode = -1;
            for (int i = 0; i < len; i++) {
                int rid = clusterData[start + i];
                if (rid >= 0 && rid < nRows) {
                    int v = recordsFlat[rid * nCols + rhs];
                    if (v >= 0 && v < maxRange) {
                        int cnt = ++histogramBuffer[base + v];
                        if (cnt > maxCount) {
                            maxCount = cnt;
                            mode = v;
                        }
                    }
                }
            }

            // Count violations (exact)
            if (mode == -1) {
                outViolations[c] = len - 1; // all but one violate if all missing/invalid
            } else {
                int viol = 0;
                for (int i = 0; i < len; i++) {
                    int rid = clusterData[start + i];
                    if (rid >= 0 && rid < nRows) {
                        if (recordsFlat[rid * nCols + rhs] != mode) viol++;
                    }
                }
                outViolations[c] = viol;
            }
        }
    }

    // ---------------------- CPU large-range path -----------------------------

    private void cpuValidateLargeRange(final int[] recordsFlat,
                                       final int nRows,
                                       final int nCols,
                                       final PartitionedClusters part) {

        // For each large-range cluster: count with a map to find mode, then violations
        final int num = part.numLargeClusters;
        part.outViolationsLarge.clear();
        part.outViolationsLarge.size(num);

        for (int c = 0; c < num; c++) {
            int start = part.clusterStartLarge.getInt(c);
            int len   = part.clusterLenLarge.getInt(c);
            int rhs   = part.rhsAttrsLarge.getInt(c);

            if (len <= 1) {
                part.outViolationsLarge.set(c, 0);
                continue;
            }

            // Count RHS values
            Int2IntOpenHashMap counts = new Int2IntOpenHashMap(Math.max(16, len * 2));
            counts.defaultReturnValue(0);

            int mode = -1, maxCnt = 0;
            for (int i = 0; i < len; i++) {
                int rid = part.clusterDataLarge.getInt(start + i);
                if (rid >= 0 && rid < nRows) {
                    int v = recordsFlat[rid * nCols + rhs];
                    int cnt = counts.addTo(v, 1) + 1;
                    if (cnt > maxCnt) { maxCnt = cnt; mode = v; }
                }
            }

            // Count violations to mode
            int viol = 0;
            for (int i = 0; i < len; i++) {
                int rid = part.clusterDataLarge.getInt(start + i);
                if (rid >= 0 && rid < nRows) {
                    if (recordsFlat[rid * nCols + rhs] != mode) viol++;
                }
            }
            part.outViolationsLarge.set(c, viol);
        }
    }

    // ----------------------- helpers & plumbing ------------------------------

    private void ensureGpuBuffers(int recordsSize,
                                  int clusterDataSmallSize,
                                  int numSmallClusters,
                                  int maxSmallRange) {
        // records
        if (d_recordsFlat == null || capRecords != recordsSize) {
            d_recordsFlat = new int[recordsSize];
            capRecords = recordsSize;
            planBuilt = false; // pointers change → rebuild plan
        }
        // cluster data
        if (d_clusterDataSmall == null || capClusterDataSmall != clusterDataSmallSize) {
            d_clusterDataSmall = new int[clusterDataSmallSize];
            capClusterDataSmall = clusterDataSmallSize;
            planBuilt = false;
        }
        if (d_clusterStartSmall == null || capClustersSmall != numSmallClusters) {
            d_clusterStartSmall = new int[numSmallClusters];
            d_clusterLenSmall   = new int[numSmallClusters];
            d_rhsAttrsSmall     = new int[numSmallClusters];
            d_outViolationsSmall= new int[numSmallClusters];
            capClustersSmall    = numSmallClusters;
            planBuilt = false;
        }
        int neededHistogram = Math.max(1, numSmallClusters * Math.max(1, maxSmallRange));
        if (d_histogramBuffer == null || capHistogram != neededHistogram) {
            d_histogramBuffer = new int[neededHistogram];
            capHistogram = neededHistogram;
            planBuilt = false;
        }
    }

    private int[] flattenRecords(int[][] compressedRecords, int nRows, int nCols) {
        int[] flat = new int[nRows * nCols];
        for (int r = 0; r < nRows; r++) {
            System.arraycopy(compressedRecords[r], 0, flat, r * nCols, nCols);
        }
        return flat;
    }

    private int[] computeColumnRanges(int[][] records, Set<Integer> columns, int nCols) {
        int[] maxVal = new int[nCols];
        for (int r = 0; r < records.length; r++) {
            int[] row = records[r];
            for (int c : columns) {
                int v = row[c];
                if (v > maxVal[c]) maxVal[c] = v;
            }
        }
        // range is max+1 (values assumed >=0); if negatives exist, adjust as needed
        for (int c : columns) maxVal[c] = maxVal[c] + 1;
        return maxVal;
    }

    private PartitionedClusters buildAndPartitionClusters(List<GPUValidator.ValidationTask> tasks, int[] rhsMaxPlus1) {
        // Small-range arrays
        IntArrayList clusterDataSmall = new IntArrayList();
        IntArrayList clusterStartSmall = new IntArrayList();
        IntArrayList clusterLenSmall   = new IntArrayList();
        IntArrayList rhsAttrsSmall     = new IntArrayList();

        // Large-range arrays
        IntArrayList clusterDataLarge = new IntArrayList();
        IntArrayList clusterStartLarge = new IntArrayList();
        IntArrayList clusterLenLarge   = new IntArrayList();
        IntArrayList rhsAttrsLarge     = new IntArrayList();

        // Task → [start,end) mapping for each partition
        int[] taskSmallStart = new int[tasks.size() + 1];
        int[] taskLargeStart = new int[tasks.size() + 1];

        int smallClustersSoFar = 0;
        int largeClustersSoFar = 0;
        int maxSmallRange = 1;

        for (int ti = 0; ti < tasks.size(); ti++) {
            taskSmallStart[ti] = smallClustersSoFar;
            taskLargeStart[ti] = largeClustersSoFar;

            GPUValidator.ValidationTask t = tasks.get(ti);
            PositionListIndex pli = t.getLhsPli();
            int rhs = t.getRhsAttr();
            int range = rhsMaxPlus1[rhs] > 0 ? rhsMaxPlus1[rhs] : SMALL_RANGE_THRESHOLD + 1;

            boolean toGPU = range <= SMALL_RANGE_THRESHOLD;

            for (IntArrayList cluster : pli.getClusters()) {
                if (toGPU) {
                    int start = clusterDataSmall.size();
                    for (int i = 0, len = cluster.size(); i < len; i++) {
                        clusterDataSmall.add(cluster.getInt(i));
                    }
                    clusterStartSmall.add(start);
                    clusterLenSmall.add(cluster.size());
                    rhsAttrsSmall.add(rhs);
                    smallClustersSoFar++;
                } else {
                    int start = clusterDataLarge.size();
                    for (int i = 0, len = cluster.size(); i < len; i++) {
                        clusterDataLarge.add(cluster.getInt(i));
                    }
                    clusterStartLarge.add(start);
                    clusterLenLarge.add(cluster.size());
                    rhsAttrsLarge.add(rhs);
                    largeClustersSoFar++;
                }
            }

            if (toGPU && range > maxSmallRange) maxSmallRange = range;
        }

        taskSmallStart[tasks.size()] = smallClustersSoFar;
        taskLargeStart[tasks.size()] = largeClustersSoFar;

        PartitionedClusters p = new PartitionedClusters();
        p.clusterDataSmall = clusterDataSmall;
        p.clusterStartSmall = clusterStartSmall;
        p.clusterLenSmall   = clusterLenSmall;
        p.rhsAttrsSmall     = rhsAttrsSmall;
        p.numSmallClusters  = smallClustersSoFar;
        p.maxSmallRange     = maxSmallRange;

        p.clusterDataLarge = clusterDataLarge;
        p.clusterStartLarge = clusterStartLarge;
        p.clusterLenLarge   = clusterLenLarge;
        p.rhsAttrsLarge     = rhsAttrsLarge;
        p.numLargeClusters  = largeClustersSoFar;

        p.taskSmallStart    = taskSmallStart;
        p.taskLargeStart    = taskLargeStart;

        p.outViolationsSmall = new IntArrayList(smallClustersSoFar);
        p.outViolationsLarge = new IntArrayList(largeClustersSoFar);
        return p;
    }

    // ------------------------ support structures -----------------------------

    private static class PartitionedClusters {
        // Small range (GPU)
        IntArrayList clusterDataSmall;
        IntArrayList clusterStartSmall;
        IntArrayList clusterLenSmall;
        IntArrayList rhsAttrsSmall;
        int numSmallClusters;
        int maxSmallRange;
        IntArrayList outViolationsSmall;

        // Large range (CPU)
        IntArrayList clusterDataLarge;
        IntArrayList clusterStartLarge;
        IntArrayList clusterLenLarge;
        IntArrayList rhsAttrsLarge;
        int numLargeClusters;
        IntArrayList outViolationsLarge;

        // Per-task mapping
        int[] taskSmallStart;
        int[] taskLargeStart;
    }

}

