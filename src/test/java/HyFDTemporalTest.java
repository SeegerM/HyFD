import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
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
    private static final String JSONL_PATH =
            "C:\\Users\\MarcianSeeger\\Downloads\\matchedWikitableHistories\\matchedWikitableHistories_new\\enwiki-20171103-pages-meta-history10xml-p3035575p3046511_wikitableHistories.json";

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
    private static final double DECAY_BASE_U = 0.8;     // U in (0,1], exponential decay (1.0 disables decay)

    // Verbosity
    private static final boolean PRINT_INTERMEDIATE_FDS = true;

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

            List<Result> results = TemporalTest.executeHyFD(gen, 1d);
            if (PRINT_INTERMEDIATE_FDS) {
                System.out.println(formatFDs(results));
            }

            Set<FDKey> holds = FDKey.extractFDs(results);

            String rid = i < meta.revIds.size() ? meta.revIds.get(i) : String.valueOf(i);
            Instant ts = i < meta.revTimes.size() ? meta.revTimes.get(i) : null;
            double weight = 1.0; // or derive from time gaps if desired

            timeline.add(new RevisionResult(rid, ts, weight, holds));
        }
        return timeline;
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

    private static void printCombinedResults(TemporalFDCombiner.Combined combined,
                                             double epsilon,
                                             long deltaRevisions,
                                             double decay) {
        System.out.println("=== STRICT tFDs ===");
        combined.strict.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== ε-relaxed tFDs (ε=" + epsilon + ") ===");
        combined.epsilonRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== (ε,δ)-relaxed tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + " revisions) ===");
        combined.epsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

        System.out.println("=== Weighted (w,ε,δ)-tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + ", w=" + decay + ") ===");
        combined.weightedEpsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));
    }
}

