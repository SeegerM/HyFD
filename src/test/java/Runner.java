import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Runner {

    static String path = "C://Users/MarcianSeeger/Documents/HyFD/data";
    public static void main(String[] args) {
        File folder = new File(Runner.path);
        File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        // Extract file names from the CSV files.
        String[] fileNames = new String[csvFiles.length];
        for (int i = 0; i < csvFiles.length; i++) {
            fileNames[i] = csvFiles[i].getName();
        }

        // Execute HyFD on all found CSV files.
        List<Result> results = executeHyFD(fileNames);
        for (Result result : results){
            System.out.println(result);
        }

    }


    public static List<Result> executeHyFD(String... names) {
        List<Result> allResults = new ArrayList<>();
        try {
            for (String fileName : names) {
                RelationalInputGenerator input = getInputGenerator(fileName);
                ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(input));

                HyFD hyFD = createHyFD(input, resultReceiver);

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
                ';',
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

    public static HyFD createHyFD(RelationalInputGenerator input, ResultCache resultReceiver) throws AlgorithmConfigurationException {
        HyFD hyFD = new HyFD();
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
        hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), "0.9");
        hyFD.setResultReceiver(resultReceiver);
        return hyFD;
    }
}


