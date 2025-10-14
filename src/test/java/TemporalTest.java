import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.*;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.algorithms.hyfd.old.OldHyFD;
import de.metanome.algorithms.hyfd.utils.InMemoryTemporalTableGenerator;
import de.metanome.algorithms.hyfd.utils.TemporalJsonFileInputGenerator;
import de.metanome.algorithms.hyfd.utils.TemporalTableInputGeneratorFactory;
import de.metanome.algorithms.hyfd.utils.multitable.FDKey;
import de.metanome.algorithms.hyfd.utils.multitable.MultiTemporalTableInputGeneratorFactory;
import de.metanome.algorithms.hyfd.utils.multitable.RevisionResult;
import de.metanome.algorithms.hyfd.utils.multitable.TemporalFDCombiner;
import de.metanome.backend.result_receiver.ResultCache;
import org.junit.Test;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class TemporalTest {

    @Test
    public void readInTest() throws IOException, InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        File temporalFile = new File("C:\\Users\\MarcianSeeger\\Downloads\\matchedWikitableHistories\\matchedWikitableHistories_new\\enwiki-20171103-pages-meta-history10xml-p3035575p3046511_wikitableHistories.json");
        TemporalTableInputGeneratorFactory factory = new TemporalTableInputGeneratorFactory(temporalFile);
        List<FileInputGenerator> tableGenerators = factory.createGenerators();

        for (FileInputGenerator generator : tableGenerators) {
            RelationalInput relationalInput = generator.generateNewCopy();
            System.out.println(relationalInput.columnNames());
            while (relationalInput.hasNext())
                System.out.println(relationalInput.next());
            break;
        }
    }

    @Test
    public void fdTest() throws IOException, InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        File temporalFile = new File("C:\\Users\\MarcianSeeger\\Downloads\\matchedWikitableHistories\\matchedWikitableHistories_new\\enwiki-20171103-pages-meta-history10xml-p3035575p3046511_wikitableHistories.json");
        TemporalTableInputGeneratorFactory factory = new TemporalTableInputGeneratorFactory(temporalFile);
        List<FileInputGenerator> tableGenerators = factory.createGenerators();

        for (FileInputGenerator generator : tableGenerators) {
            List<Result> results = executeHyFD(generator, 1d);
            for (Result r1 : results){
                System.out.println(r1);
            }
            break;
        }
    }

    @Test
    public void fdTestMulti() throws IOException {
        DateTimeFormatter tsFmt = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH).withZone(ZoneOffset.UTC);
        File temporalFile = new File("C:\\Users\\MarcianSeeger\\Downloads\\matchedWikitableHistories\\matchedWikitableHistories_new\\enwiki-20171103-pages-meta-history10xml-p3035575p3046511_wikitableHistories.json");

        try (BufferedReader reader = new BufferedReader(new FileReader(temporalFile))) {
            String jsonLine;
            while ((jsonLine = reader.readLine()) != null) {
                if (jsonLine.trim().isEmpty()) {
                    continue;
                }

                JsonNode rootNode = new ObjectMapper().readTree(jsonLine);
                String pageId = rootNode.get("pageID").asText();
                String tableId = rootNode.get("tableID").asText();
                String uniqueTableName = pageId + "_" + tableId;
                System.out.println("Table("+uniqueTableName+"):");
                JsonNode revisions = rootNode.path("tables");
                List<String> revIds = new ArrayList<>();
                List<Instant> revTimes = new ArrayList<>();
                for (JsonNode rev : revisions) {
                    String rid = rev.has("revisionID") ? rev.get("revisionID").asText() : null;
                    String rdate = rev.has("revisionDate") ? rev.get("revisionDate").asText() : null;
                    Instant ts = null;
                    if (rdate != null && !rdate.isEmpty()) {
                        try {
                            TemporalAccessor ta = tsFmt.parse(rdate);
                            ts = Instant.from(ta);
                        } catch (Exception ignored) {}
                    }
                    revIds.add(rid);
                    revTimes.add(ts);
                }

                // Pass the entire JSON line (which represents one table's history)
                // to a specialized, in-memory generator.
                MultiTemporalTableInputGeneratorFactory factory = new MultiTemporalTableInputGeneratorFactory(jsonLine);
                List<FileInputGenerator> tableGenerators = factory.createGenerators();
                List<RevisionResult> timeline = new ArrayList<>();
                for (int i = 0; i < tableGenerators.size(); i++) {
                    FileInputGenerator gen = tableGenerators.get(i);
                    List<Result> results = executeHyFD(gen, 1d);
                    Set<FDKey> holds = FDKey.extractFDs(results);
                    System.out.println(
                            results.stream()
                                    .filter(r -> r instanceof FunctionalDependency)
                                    .map(r -> {
                                        FunctionalDependency fd = (FunctionalDependency) r;
                                        String lhs = fd.getDeterminant().getColumnIdentifiers().stream()
                                                .map(ColumnIdentifier::getColumnIdentifier)
                                                .collect(java.util.stream.Collectors.joining(","));
                                        String rhs = fd.getDependant().getColumnIdentifier();
                                        return lhs + " -> " + rhs;
                                    })
                                    .collect(java.util.stream.Collectors.joining("; "))
                    );

                    String rid = (i < revIds.size() && revIds.get(i) != null) ? revIds.get(i) : String.valueOf(i);
                    Instant ts = (i < revTimes.size()) ? revTimes.get(i) : null;
                    double weight = 1.0; // or derive from time gaps, e.g., next_ts - ts

                    timeline.add(new RevisionResult(rid, ts, weight, holds, null));
                }

                double epsilon = 0.2; // allow 10% violations
                boolean useIndexDelta = true;
                long deltaRevisions = 1; // δ=1 → neighbors t-1..t+1
                Duration deltaTimeWindow = Duration.ofHours(0); // not used when useIndexDelta=true
                double weightedEpsilon = epsilon; // set >0 to allow weighted violations
                double U = 0.8; // e.g., newer timestamps 10% more important than their immediate predecessor

                TemporalFDCombiner.Combined combined = TemporalFDCombiner.combine(
                        timeline, epsilon, useIndexDelta, deltaRevisions, deltaTimeWindow, weightedEpsilon, U
                );


                // Print results
                System.out.println("=== STRICT tFDs ===");
                combined.strict.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

                System.out.println("=== ε-relaxed tFDs (ε=" + epsilon + ") ===");
                combined.epsilonRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

                System.out.println("=== (ε,δ)-relaxed tFDs (ε=" + epsilon + ", δ=" + deltaRevisions + " revisions) ===");
                combined.epsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));

                System.out.println("=== Weighted (w,ε,δ)-tFDs (ε=" + weightedEpsilon + ", δ=" + deltaRevisions + ") ===");
                combined.weightedEpsilonDeltaRelaxed.forEach(fd -> System.out.println(fd + "  [" + combined.notes.get(fd) + "]"));


                break;
            }
        }
    }

    public static List<Result> executeHyFD(FileInputGenerator input, double value) {
        List<Result> allResults = new ArrayList<>();
        try {
            List<ColumnIdentifier> acceptedColumns = getAcceptedColumns(input);
            if (acceptedColumns.isEmpty())
                return allResults;

            ResultCache resultReceiver = new ResultCache("MetanomeMock", acceptedColumns);

            OldHyFD hyFD = createHyFD(value, input, resultReceiver);

            long time = System.currentTimeMillis();
            hyFD.execute();
            time = System.currentTimeMillis() - time;

            List<Result> results = resultReceiver.fetchNewResults();
            allResults.addAll(results);
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
        return allResults;
    }

    public static OldHyFD createHyFD(double value, RelationalInputGenerator input, ResultCache resultReceiver) throws AlgorithmConfigurationException {
        OldHyFD hyFD = new OldHyFD();
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
        hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), ""+value);//96,80
        hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), -1);
        hyFD.setResultReceiver(resultReceiver);
        return hyFD;
    }

    public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();
        List<String> columnNames = relationalInput.columnNames();
        if (columnNames == null)
            return acceptedColumns;
        for (String columnName : columnNames)
            acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
        return acceptedColumns;
    }

}
