import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.*;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.algorithms.hyfd.old.OldHyFD;
import de.metanome.algorithms.hyfd.utils.TemporalJsonFileInputGenerator;
import de.metanome.algorithms.hyfd.utils.TemporalTableInputGeneratorFactory;
import de.metanome.backend.result_receiver.ResultCache;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
