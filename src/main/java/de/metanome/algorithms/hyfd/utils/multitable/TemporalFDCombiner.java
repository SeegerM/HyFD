package de.metanome.algorithms.hyfd.utils.multitable;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class TemporalFDCombiner {

    public static class Combined {
        public final Set<FDKey> strict = new HashSet<>();
        public final Set<FDKey> epsilonRelaxed = new HashSet<>();
        public final Set<FDKey> epsilonDeltaRelaxed = new HashSet<>();
        public final Set<FDKey> weightedEpsilonDeltaRelaxed = new HashSet<>();
        // Optional: store per-FD diagnostics
        public final Map<FDKey, String> notes = new HashMap<>();

        // Q_kon consistency scores per FD (based on timeline)
        public final Map<FDKey, Double> qSimple = new HashMap<>();   // r_t = ¬holds[t],  g_t = 1
        public final Map<FDKey, Double> qDelta  = new HashMap<>();   // r_t = not δ-determined, g_t = 1
        public final Map<FDKey, Double> qWeighted = new HashMap<>(); // r_t = not δ-determined, g_t = normalized effW[t]

        // per-timestamp quality (aligned with the timeline order)
        public final List<Instant> timeAxis = new ArrayList<>();
        public final List<Double> qTimeSimple = new ArrayList<>();
        public final List<Double> qTimeDelta = new ArrayList<>();
        public final List<Double> qTimeWeighted = new ArrayList<>();

        // health
        public final List<Set<FDKey>> holdsAt = new ArrayList<>(); // per-timestamp holds
        public final List<int[]> neighborRanges = new ArrayList<>(); // per t: [lo, hi]

    }

    /** Combine per-revision FD results.
     *
     * @param timeline            ordered list of revision results (ascending time/index)
     * @param epsilon             allowed violation share (0..1) for ε and (ε,δ)
     * @param useIndexDelta       true: δ is number of neighbor revisions; false: δ is time window
     * @param deltaWindow         if useIndexDelta: integer window (e.g., 1 means t-1..t+1); if time: duration
     * @param weightedEpsilon     threshold for weighted variant (sum of weights of “not δ-determined”)
     * @return Combined results
     */
    public static Combined combine(List<RevisionResult> timeline,
                                   double epsilon,
                                   boolean useIndexDelta,
                                   long deltaWindow,               // if useIndexDelta==true
                                   Duration deltaTimeWindow,       // else
                                   double weightedEpsilon) {
        return combine(timeline, epsilon, useIndexDelta, deltaWindow, deltaTimeWindow,
                weightedEpsilon, 1.0 /*decayBaseU*/);
    }

    /**
     * Decay-aware overload. The weighted (w,ε,δ) rule uses effective weights
     *   effW(t) = timeline[t].weight * decay(t), where decay(t) = U^(T-1 - t), U in (0,1].
     * With U=1.0 this reduces to the legacy behavior (no decay).
     *
     * @param timeline         ordered list of revision results (ascending time/index)
     * @param epsilon          allowed violation share (0..1) for ε and (ε,δ)
     * @param useIndexDelta    true: δ is number of neighbor revisions; false: δ is time window
     * @param deltaWindow      if useIndexDelta: integer window (e.g., 1→t-1..t+1); if time: ignored
     * @param deltaTimeWindow  if !useIndexDelta: time window half-width; else ignored
     * @param weightedEpsilon  threshold for weighted variant (absolute sum of effW over non-δ-determined t)
     * @param decayBaseU       exponential decay base U in (0,1]; newer timestamps weigh more
     */
    public static Combined combine(List<RevisionResult> timeline,
                                   double epsilon,
                                   boolean useIndexDelta,
                                   long deltaWindow,                 // if useIndexDelta==true
                                   Duration deltaTimeWindow,         // else
                                   double weightedEpsilon,
                                   double decayBaseU) {
        Combined out = new Combined();
        if (timeline.isEmpty()) return out;

        // Universe of all FDs ever seen (union across revisions)
        Set<FDKey> universe = new HashSet<>();
        for (RevisionResult rr : timeline) {
            universe.addAll(rr.holds);
            out.holdsAt.add(new HashSet<>(rr.holds));
        }

        int T = timeline.size();
        //double totalWeight = timeline.stream().mapToDouble(rr -> rr.weight).sum();

        Map<FDKey, Double> gFD = new HashMap<>();
        for (RevisionResult rr : timeline) {
            rr.fdWeights.forEach((fd, w) -> gFD.merge(fd, w, Math::max)); // max over time
        }

        // Precompute: for time-window δ we need neighbor indices per t
        List<int[]> neighborRanges = new ArrayList<>(T);
        for (int t = 0; t < T; t++) {
            int lo, hi;
            if (useIndexDelta) {
                int d = (int) deltaWindow;
                lo = Math.max(0, t - d);
                hi = Math.min(T - 1, t + d);
            } else {
                Instant center = timeline.get(t).timestamp;
                if (center == null) { // fallback: treat as δ=0 in index terms
                    lo = t; hi = t;
                } else {
                    Instant loTs = center.minus(deltaTimeWindow);
                    Instant hiTs = center.plus(deltaTimeWindow);
                    lo = t; hi = t;
                    // expand while inside window
                    for (int i = t - 1; i >= 0; i--) {
                        Instant ts = timeline.get(i).timestamp;
                        if (ts != null && !ts.isBefore(loTs)) lo = i; else break;
                    }
                    for (int i = t + 1; i < T; i++) {
                        Instant ts = timeline.get(i).timestamp;
                        if (ts != null && !ts.isAfter(hiTs)) hi = i; else break;
                    }
                }
            }
            neighborRanges.add(new int[]{lo, hi});
        }
        out.neighborRanges.addAll(neighborRanges);

        // Precompute decay weights (newest gets decay=1)
        final double[] decay = new double[T];
        if (decayBaseU <= 0.0 || decayBaseU > 1.0) {
            throw new IllegalArgumentException("decayBaseU must be in (0,1], got " + decayBaseU);
        }
        if (decayBaseU == 1.0) {
            Arrays.fill(decay, 1.0);
        } else {
            for (int t = 0; t < T; t++) {
                decay[t] = Math.pow(decayBaseU, (T - 1 - t));
            }
        }

        // Precompute effective weights: effW(t) = timeline[t].weight * decay[t]
        final double[] effW = new double[T];
        double totalEffW = 0.0;
        for (int t = 0; t < T; t++) {
            double w = timeline.get(t).weight;
            double ew = w * decay[t];
            effW[t] = ew;
            totalEffW += ew;
        }

        final int[] violPerT = new int[T];        // count of FDs violated at t
        final int[] notDeltaPerT = new int[T];    // count of FDs not δ-determined at t
        final double[] weightedPerT = new double[T]; // sum of g_fd over not-δ FDs at t

        // For Q_weighted we need g_t in (0,1]; normalize effW to (0,1]
        double maxEffW = 0.0;
        for (double v : effW) if (v > maxEffW) maxEffW = v;
        final double[] gWeight = new double[T];
        for (int t = 0; t < T; t++) gWeight[t] = (maxEffW > 0.0) ? (effW[t] / maxEffW) : 1.0;

        for (FDKey fd : universe) {

            // Booleans per t whether FD holds AT t
            boolean[] holds = new boolean[T];
            for (int t = 0; t < T; t++) {
                holds[t] = timeline.get(t).holds.contains(fd);
                if (!holds[t]) violPerT[t]++;
            }


            // strict: holds at all t
            boolean isStrict = true;
            for (boolean h : holds) {
                if (!h) {
                    isStrict = false;
                    break;
                }
            }
            if (isStrict) out.strict.add(fd);

            // ε-relaxed: violations/|T| <= ε
            int viol = 0;
            for (boolean h : holds) if (!h) viol++;
            double violShare = (double) viol / (double) T;
            if (violShare <= epsilon) out.epsilonRelaxed.add(fd);

            // (ε,δ)-relaxed: count t where NOT δ-determined:
            // FD must hold at t OR anywhere in window I=[t-δ, t+δ]
            int notDeltaDetermined = 0;
            for (int t = 0; t < T; t++) {
                if (holds[t]) continue;
                int[] rng = neighborRanges.get(t);
                boolean found = false;
                for (int i = rng[0]; i <= rng[1]; i++) {
                    if (holds[i]) { found = true; break; }
                }
                if (!found) {
                    notDeltaDetermined++;
                    notDeltaPerT[t]++;
                    //if (((double) notDeltaDetermined / T) > epsilon) break;
                }
            }
            double notDeltaShare = (double) notDeltaDetermined / (double) T;
            if (notDeltaShare <= epsilon) out.epsilonDeltaRelaxed.add(fd);

            // weighted (w,ε,δ) with exponential decay: sum effW(t) over NOT δ-determined t
            double wsum = 0.0;
            for (int t = 0; t < T; t++) {
                if (holds[t]) continue;
                int[] rng = neighborRanges.get(t);
                boolean found = false;
                for (int i = rng[0]; i <= rng[1]; i++) {
                    if (holds[i]) { found = true; break; }
                }
                if (!found) {
                    wsum += effW[t];
                    //double gFD = 1.0; // @TODO replace with gpdep?
                    weightedPerT[t] += gFD.getOrDefault(fd, 1.0);
                    //weightedPerT[t]  += gFD;
                    //if (wsum > weightedEpsilon) break; // early stop
                }
            }
            if (wsum <= weightedEpsilon) {
                out.weightedEpsilonDeltaRelaxed.add(fd);
            }

            // ---- Q_kon scores --------------------------------------------------------
            // Q_simple: r_t = 1 if FD does NOT hold at t; g_t = 1
            double qSimple = 1.0 / (viol + 1.0);

            // Q_delta: r_t = 1 if FD is NOT δ-determined at t; g_t = 1
            double qDelta = 1.0 / (notDeltaDetermined + 1.0);

            // Q_weighted: r_t = 1 if NOT δ-determined; g_t = normalized effW[t] in (0,1]
            double sumWeightedViol = 0.0;
            for (int t = 0; t < T; t++) {
                if (holds[t]) continue;
                int[] rng = neighborRanges.get(t);
                boolean found = false;
                for (int i = rng[0]; i <= rng[1]; i++) {
                    if (holds[i]) { found = true; break; }
                }
                if (!found) sumWeightedViol += gWeight[t];
            }
            double qWeighted = 1.0 / (sumWeightedViol + 1.0);

            // Store
            out.qSimple.put(fd, qSimple);
            out.qDelta.put(fd, qDelta);
            out.qWeighted.put(fd, qWeighted);

            // Diagnostics
            out.notes.put(fd, String.format(
                    Locale.ROOT,
                    "viol=%d/%d (%.3f), notDelta=%d/%d (%.3f), w-viol=%.6f",
                    viol, T, violShare,
                    notDeltaDetermined, T, notDeltaShare,
                    wsum));
        }

        for (int t = 0; t < T; t++) {
            out.timeAxis.add(timeline.get(t).timestamp);
            out.qTimeSimple.add(1.0 / (violPerT[t] + 1.0));
            out.qTimeDelta.add(1.0 / (notDeltaPerT[t] + 1.0));
            out.qTimeWeighted.add(1.0 / (weightedPerT[t] + 1.0));
        }

        return out;
    }
}
