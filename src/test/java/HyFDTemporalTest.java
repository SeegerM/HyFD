import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.utils.multitable.FDKey;
import de.metanome.algorithms.hyfd.utils.multitable.MultiTemporalTableInputGeneratorFactory;
import de.metanome.algorithms.hyfd.utils.multitable.RevisionResult;
import de.metanome.algorithms.hyfd.utils.multitable.TemporalFDCombiner;
import org.junit.Test;

import java.io.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HyFDTemporalTest {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    private static final String JSONL_PATH =
            "data\\enwiki-20171103-pages-meta-history10xml-p3035575p3046511_wikitableHistories.json";

    // Timestamp parser (matches examples like: "Thu Jan 09 10:53:23 CET 2014")
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH)
                    .withZone(ZoneOffset.UTC);

    // Temporal FD parameters
    private static final double EPSILON = 0.2;          // ε for share-based relaxations
    private static final boolean USE_INDEX_DELTA = true; // true: δ = #neighbor revisions; false: δ = time window
    private static final long DELTA_REVISIONS = 1;       // δ (index mode): 1 → t-1..t+1
    private static final Duration DELTA_TIME_WINDOW = Duration.ofHours(0); // (time mode)
    private static final double WEIGHTED_EPSILON = EPSILON; // absolute budget for weighted violations
    private static final double DECAY_BASE_U = 0.77;     // U in (0,1], exponential decay (1.0 disables decay)
    private static final double PARTIAL_FACTOR = 1d;    // Partial Factor for FD discovery

    // Verbosity
    private static final boolean PRINT_INTERMEDIATE_FDS = true;
    private static final boolean USE_GPDEP = true;

    @Test
    public void wikiTableTest() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(JSONL_PATH))) {
            String jsonLine;
            while ((jsonLine = reader.readLine()) != null) {
                if (jsonLine.trim().isEmpty()) continue;

                JsonNode root = new ObjectMapper().readTree(jsonLine);
                String uniqueTableName = getUniqueTableName(root);
                System.out.println("Table(" + uniqueTableName + "):");

                // Parse revision IDs + timestamps once
                RevisionMeta meta = parseRevisionMeta(root, TS_FMT);

                // Build per-revision generators and run HyFD
                List<RevisionResult> timeline = buildTimeline(jsonLine, meta);

                // Combine according to parameters
                TemporalFDCombiner.Combined combined = TemporalFDCombiner.combine(
                        timeline,
                        EPSILON,
                        USE_INDEX_DELTA,
                        DELTA_REVISIONS,
                        DELTA_TIME_WINDOW,
                        WEIGHTED_EPSILON,
                        DECAY_BASE_U
                );

                // Print summary
                printCombinedResults(combined, EPSILON, DELTA_REVISIONS, DECAY_BASE_U);

                break;
            }
        }
    }

    @Test
    public void temporalRuntimeTest() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(JSONL_PATH))) {
            String jsonLine;
            while ((jsonLine = reader.readLine()) != null) {
                if (jsonLine.trim().isEmpty()) continue;

                JsonNode root = new ObjectMapper().readTree(jsonLine);
                String uniqueTableName = getUniqueTableName(root);

                long elapsedMs = runtime(jsonLine);

                System.out.printf("Table(%s): %d ms%n", uniqueTableName, elapsedMs);


            }
        }
    }

    // =================
    // Helper structures
    // =================
    private static final class RevisionMeta {
        final List<String> revIds;
        final List<Instant> revTimes;
        RevisionMeta(List<String> revIds, List<Instant> revTimes) {
            this.revIds = revIds; this.revTimes = revTimes;
        }
    }

    // ===============
    // Helper methods
    // ===============
    private static String getUniqueTableName(JsonNode root) {
        String pageId = optText(root, "pageID", "page");
        String tableId = optText(root, "tableID", "table");
        return pageId + "_" + tableId;
    }

    private static RevisionMeta parseRevisionMeta(JsonNode root, DateTimeFormatter fmt) {
        JsonNode revisions = root.path("tables");
        List<String> revIds = new ArrayList<>();
        List<Instant> revTimes = new ArrayList<>();

        for (JsonNode rev : revisions) {
            String rid = rev.has("revisionID") ? rev.get("revisionID").asText() : null;
            String rdate = rev.has("revisionDate") ? rev.get("revisionDate").asText() : null;
            Instant ts = parseInstantSafe(rdate, fmt);
            revIds.add(rid != null ? rid : String.valueOf(revIds.size()));
            revTimes.add(ts);
        }
        return new RevisionMeta(revIds, revTimes);
    }

    private static Instant parseInstantSafe(String rdate, DateTimeFormatter fmt) {
        if (rdate == null || rdate.isEmpty()) return null;
        try {
            TemporalAccessor ta = fmt.parse(rdate);
            return Instant.from(ta);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String optText(JsonNode n, String key, String fallback) {
        JsonNode v = n.get(key);
        return (v != null && !v.isNull()) ? v.asText() : fallback;
    }

    private static long runtime(String jsonLine) throws IOException {
        long startNs = System.nanoTime();

        MultiTemporalTableInputGeneratorFactory factory = new MultiTemporalTableInputGeneratorFactory(jsonLine);
        List<FileInputGenerator> perRevisionGenerators = factory.createGenerators();

        for (int i = 0; i < perRevisionGenerators.size(); i++) {
            FileInputGenerator gen = perRevisionGenerators.get(i);
            List<Result> results = TemporalTest.executeHyFD(gen, 1d);
        }

        long endNs = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endNs - startNs);
    }

    private static List<RevisionResult> buildTimeline(String jsonLine, RevisionMeta meta) throws IOException {
        MultiTemporalTableInputGeneratorFactory factory = new MultiTemporalTableInputGeneratorFactory(jsonLine);
        List<FileInputGenerator> perRevisionGenerators = factory.createGenerators();

        List<RevisionResult> timeline = new ArrayList<>(perRevisionGenerators.size());
        for (int i = 0; i < perRevisionGenerators.size(); i++) {
            FileInputGenerator gen = perRevisionGenerators.get(i);

            // 1) Run HyFD
            List<Result> results = TemporalTest.executeHyFD(gen, PARTIAL_FACTOR);
            if (PRINT_INTERMEDIATE_FDS) System.out.println(formatFDs(results));

            // 2) Extract FDs that hold
            Set<FDKey> holds = FDKey.extractFDs(results);

            // 3) Compute gpdep weights from THIS generator
            Map<FDKey, Double> fdWeights = Collections.emptyMap();
            if(USE_GPDEP) {
                try {
                    fdWeights = gpdepWeightsForRevision(results, gen);
                } catch (Exception ex) {
                    System.err.println("gpdep failed at rev " + i + ": " + ex.getMessage());
                }
            }

            // 4) Metadata
            String rid = i < meta.revIds.size() ? meta.revIds.get(i) : String.valueOf(i);
            Instant ts = i < meta.revTimes.size() ? meta.revTimes.get(i) : null;
            double timeWeight = 1.0;

            // 5) Add to timeline (RevisionResult extended to accept fdWeights)
            timeline.add(new RevisionResult(rid, ts, timeWeight, holds, fdWeights));
        }
        return timeline;
    }

    private static FDKey toFDKey(RelaxedFunctionalDependency rfd) {
        List<ColumnIdentifier> lhs = new ArrayList<>(rfd.getDeterminant().getColumnIdentifiers());
        ColumnIdentifier rhs = rfd.getDependant();
        return new FDKey(lhs, rhs);
    }

    private static Map<FDKey, Double> gpdepWeightsForRevision(List<Result> results,
                                                              FileInputGenerator gen) {
        // Keep only Relaxed FDs
        List<Result> relaxed = results.stream()
                .filter(r -> r instanceof RelaxedFunctionalDependency)
                .collect(Collectors.toList());

        // Compute gpdep for each (RFD, gpdep)
        List<Pair<RelaxedFunctionalDependency, PdepTuple>> pdeps =
                MetadataUtils.getPdeps(relaxed, gen);

        // Aggregate to FDKey -> raw gpdep (keep the max if duplicates)
        Map<FDKey, Double> raw = new HashMap<>();
        for (Pair<RelaxedFunctionalDependency, PdepTuple> pair : pdeps) {
            FDKey key = toFDKey(pair.getFirst());
            double g = pair.getSecond().gpdep;
            raw.merge(key, g, Double::max);
        }

        // Normalize per revision to [0,1]
        double maxPos = raw.values().stream().mapToDouble(v -> Math.max(0.0, v)).max().orElse(0.0);
        Map<FDKey, Double> norm = new HashMap<>();
        for (Map.Entry<FDKey, Double> e : raw.entrySet()) {
            double v = Math.max(0.0, e.getValue());           // floor negatives
            double w = (maxPos > 0.0) ? (v / maxPos) : 0.0;   // all non-positive → 0
            norm.put(e.getKey(), w);
        }
        return norm;
    }

    private static String formatFDs(List<Result> results) {
        return results.stream()
                .filter(r -> r instanceof FunctionalDependency)
                .map(r -> {
                    FunctionalDependency fd = (FunctionalDependency) r;
                    String lhs = fd.getDeterminant().getColumnIdentifiers().stream()
                            .map(ColumnIdentifier::getColumnIdentifier)
                            .collect(Collectors.joining(","));
                    String rhs = fd.getDependant().getColumnIdentifier();
                    return lhs + " -> " + rhs;
                })
                .collect(Collectors.joining("; "));
    }

    private static void printCombinedResultsWithoutConsistency(TemporalFDCombiner.Combined combined,
                                             double epsilon,
                                             long deltaRevisions,
                                             double decay) {
        System.out.println(ANSI_RED + "=== STRICT tFDs ===" + ANSI_RESET);
        combined.strict.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== ε-relaxed tFDs (ε=" + epsilon + ") ===");
        combined.epsilonRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== (ε,δ)-relaxed tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + " revisions) ===");
        combined.epsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== Weighted (w,ε,δ)-tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + ", w=" + decay + ") ===");
        combined.weightedEpsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== Top by Qweighted ===");
        combined.qWeighted.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue())) // highest first
                .limit(20)
                .forEach(e -> System.out.println(e.getKey() + "  [Qweighted=" +
                        String.format(Locale.ROOT, "%.3f", e.getValue()) + "]"));
    }

    private static void printCombinedResults(TemporalFDCombiner.Combined combined,
                                             double epsilon,
                                             long deltaRevisions,
                                             double decay) {
        System.out.println();
        System.out.println(ANSI_RED + "=== STRICT tFDs ===" + ANSI_RESET);
        combined.strict.forEach(fd ->
                System.out.println(fd + "  " + fmtQ(combined, fd)));
        if (combined.strict.isEmpty()) System.out.println("(empty)");

        System.out.println();
        System.out.println(ANSI_RED +"=== ε-relaxed tFDs (ε=" + epsilon + ") ===" + ANSI_RESET);
        combined.epsilonRelaxed.forEach(fd ->
                System.out.println(fd + "  " + fmtQ(combined, fd)));
        if (combined.epsilonRelaxed.isEmpty()) System.out.println("(empty)");

        System.out.println();
        System.out.println(ANSI_RED +"=== (ε,δ)-relaxed tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + " revisions) ===" + ANSI_RESET);
        combined.epsilonDeltaRelaxed.forEach(fd ->
                System.out.println(fd + "  " + fmtQ(combined, fd)));
        if (combined.epsilonDeltaRelaxed.isEmpty()) System.out.println("(empty)");

        System.out.println();
        System.out.println(ANSI_RED +"=== Weighted (w,ε,δ)-tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + ", w=" + decay + ") ===" + ANSI_RESET);
        combined.weightedEpsilonDeltaRelaxed.forEach(fd ->
                System.out.println(fd + "  " + fmtQ(combined, fd)));
        if (combined.weightedEpsilonDeltaRelaxed.isEmpty()) System.out.println("(empty)");

        System.out.println();
        System.out.println(ANSI_RED +"=== Top by Qweighted ===" + ANSI_RESET);
        combined.qWeighted.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue())) // highest first
                .limit(20)
                .forEach(e -> System.out.println(e.getKey() + "  [Qweighted=" +
                        String.format(Locale.ROOT, "%.3f", e.getValue()) + "]"));

        System.out.println();
        System.out.println(ANSI_RED +"=== Overall table consistency scores ===" + ANSI_RESET);
        double qAll      = overallConsistency(combined, null, QMode.SIMPLE, false, null);
        double qAllED    = overallConsistency(combined, null, QMode.DELTA, false, null);
        double qAllW     = overallConsistency(combined, null, QMode.WEIGHTED, false, null);
        double qEpsDel   = overallConsistency(combined, combined.epsilonDeltaRelaxed, QMode.DELTA, false, null);
        double qWeighted = overallConsistency(combined, combined.weightedEpsilonDeltaRelaxed, QMode.WEIGHTED, false, null);

        System.out.printf(Locale.ROOT, "Overall Q(table) over ALL FDs (strict)     : %.3f%n", qAll);
        System.out.printf(Locale.ROOT, "Overall Q(table) over ALL FDs (ε,δ)        : %.3f%n", qAllED);
        System.out.printf(Locale.ROOT, "Overall Q(table) over ALL FDs (weighted)   : %.3f%n", qAllW);
        System.out.printf(Locale.ROOT, "Overall Q(table) over (ε,δ)-FDs            : %.3f%n", qEpsDel);
        System.out.printf(Locale.ROOT, "Overall Q(table) over weighted (w,ε,δ)-FDs : %.3f%n", qWeighted);

        System.out.println();
        printPerTimestampConsistency(combined);

        Set<FDKey> scope = combined.weightedEpsilonDeltaRelaxed; // or epsilonDeltaRelaxed, or final state
        List<ScopedTimeScore> scores = perTimestampScopedScores(combined, scope,
                true,   // δ-aware
                1.0,    // F1
                1.0, 1.0); // equal FP/FN penalties

        System.out.println();
        System.out.println(ANSI_RED +"=== Per-timestamp consistency vs scope (penalizing extras) ==="+ ANSI_RESET);
        for (int i = 0; i < scores.size(); i++) {
            Instant ts = combined.timeAxis.get(i);
            ScopedTimeScore s = scores.get(i);
            System.out.printf(Locale.ROOT,
                    "%s  prec=%.3f rec=%.3f F1=%.3f J=%.3f Qpen=%.3f  [TP=%d FP=%d FN=%d |S|=%d |P|=%d]%n",
                    ts != null ? ts.toString() : ("t="+i),
                    s.precision, s.recall, s.fbeta, s.jaccard, s.qPenalized,
                    s.tp, s.fp, s.fn, s.sizeS, s.sizeP
            );
        }

    }

    private static void printPerTimestampConsistency(TemporalFDCombiner.Combined c) {
        System.out.println(ANSI_RED +"=== Per-timestamp consistency (Q_time) ===" + ANSI_RESET);
        for (int i = 0; i < c.qTimeWeighted.size(); i++) {
            Instant ts = (i < c.timeAxis.size()) ? c.timeAxis.get(i) : null;
            String tsStr = (ts == null) ? ("t=" + i) : ts.toString();
            System.out.printf(
                    Locale.ROOT,
                    "%s  [Qsimple=%.3f, Qdelta=%.3f, Qweighted=%.3f]%n",
                    tsStr, c.qTimeSimple.get(i), c.qTimeDelta.get(i), c.qTimeWeighted.get(i)
            );
        }
    }


    private static String fmtQ(TemporalFDCombiner.Combined c, FDKey fd) {
        Double qS = c.qSimple.get(fd);
        Double qD = c.qDelta.get(fd);
        Double qW = c.qWeighted.get(fd);
        String note = c.notes.getOrDefault(fd, "");
        return String.format(
                Locale.ROOT,
                "[Qsimple=%.3f, Qdelta=%.3f, Qweighted=%.3f, %s]",
                qS != null ? qS : Double.NaN,
                qD != null ? qD : Double.NaN,
                qW != null ? qW : Double.NaN,
                note
        ).trim();
    }

    enum QMode { SIMPLE, DELTA, WEIGHTED }

    private static double overallConsistency(
            TemporalFDCombiner.Combined c,
            Set<FDKey> scope,              // e.g., c.epsilonDeltaRelaxed (may be null → all)
            QMode mode,                    // which per-FD Q to use
            boolean complexityAware,       // true → weight by 1/|LHS|
            Map<FDKey, Integer> lhsSizes   // optional: size cache (null → treat all sizes as 1)
    ) {
        Map<FDKey, Double> qMap = switch (mode) {
            case SIMPLE   -> c.qSimple;
            case DELTA    -> c.qDelta;
            case WEIGHTED -> c.qWeighted;
        };

        // Default scope = all FDs we have Q for
        Set<FDKey> S = (scope == null || scope.isEmpty()) ? qMap.keySet() : scope;

        double num = 0.0, den = 0.0;
        for (FDKey fd : S) {
            Double q = qMap.get(fd);
            if (q == null) continue;

            double w = 1.0;
            if (complexityAware && lhsSizes != null) {
                Integer k = lhsSizes.get(fd);
                if (k != null && k > 0) w = 1.0 / k;
            }
            num += w * q;
            den += w;
        }
        return den == 0.0 ? 1.0 : num / den;
    }

    static final class ScopedTimeScore {
        final double precision, recall, fbeta, jaccard, qPenalized;
        final int tp, fp, fn, sizeS, sizeP;
        ScopedTimeScore(double p, double r, double f, double j, double q,
                        int tp, int fp, int fn, int sizeS, int sizeP) {
            this.precision=p; this.recall=r; this.fbeta=f; this.jaccard=j; this.qPenalized=q;
            this.tp=tp; this.fp=fp; this.fn=fn; this.sizeS=sizeS; this.sizeP=sizeP;
        }
    }

    static List<ScopedTimeScore> perTimestampScopedScores(
            TemporalFDCombiner.Combined c,
            Set<FDKey> scope,       // gold S; if null/empty -> use c.weightedEpsilonDeltaRelaxed
            boolean deltaAware,     // use δ-window to build P_t
            double beta,            // for F_beta (beta=1 for F1)
            double wFP, double wFN  // penalties in Q^{pen}
    ) {
        Set<FDKey> S = (scope == null || scope.isEmpty())
                ? c.weightedEpsilonDeltaRelaxed
                : scope;
        int T = c.holdsAt.size();
        List<ScopedTimeScore> out = new ArrayList<>(T);
        int sizeS = S.size();

        for (int t = 0; t < T; t++) {
            // Build P_t (δ-aware if requested)
            Set<FDKey> Pt = new HashSet<>();
            if (deltaAware) {
                int[] rng = c.neighborRanges.get(t);
                for (int i = rng[0]; i <= rng[1]; i++) Pt.addAll(c.holdsAt.get(i));
            } else {
                Pt.addAll(c.holdsAt.get(t));
            }
            int sizeP = Pt.size();

            // Confusion-style counts
            int tp = 0;
            for (FDKey fd : S) if (Pt.contains(fd)) tp++;
            int fp = 0;
            for (FDKey fd : Pt) if (!S.contains(fd)) fp++;
            int fn = sizeS - tp;

            // Metrics
            double prec = (sizeP == 0) ? (sizeS == 0 ? 1.0 : 0.0) : (double) tp / sizeP;
            double rec  = (sizeS == 0) ? 1.0 : (double) tp / sizeS;
            double denom = (beta*beta)*prec + rec;
            double fbeta = (prec == 0.0 && rec == 0.0) ? 0.0 : ((1+beta*beta) * prec * rec) / Math.max(denom, 1e-12);
            int union = sizeP + sizeS - tp;
            double jacc = (union == 0) ? 1.0 : (double) tp / union;

            double qPen = 1.0 / (wFP * fp + wFN * fn + 1.0);

            out.add(new ScopedTimeScore(prec, rec, fbeta, jacc, qPen, tp, fp, fn, sizeS, sizeP));
        }
        return out;
    }
}

