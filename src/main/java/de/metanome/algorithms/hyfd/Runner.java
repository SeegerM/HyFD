package de.metanome.algorithms.hyfd;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.uni_potsdam.hpi.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Runner {

    static String resultFileName = "results.txt";
    static String datasetFolder = "F:\\metaserve\\io\\data\\";
    static String datasetName;
    static char separator = ',';
    static String fileEnding = ".csv";
    static boolean inputFileHasHeader = true;
    static double threshold = 1.0;
    static String nullString = "";
    static char quoteChar = '\'';
    static char escapeChar = '\\';
    static boolean strictQuots = false;
    static String[] relationNames;

    public static void main(String[] args) {
        if (args.length < 3) {
            inputFileHasHeader = true;
            fileEnding = ".csv";
            separator = ',';
            datasetName = "Cars"; //"TPC-H Test";
        } else {
            datasetName = args[0];
            separator = args[1].charAt(0);
            fileEnding = String.valueOf(args[2]);
            inputFileHasHeader = Boolean.parseBoolean(args[3]);
            threshold = Double.parseDouble(args[4]);
        }
        if (datasetName.equals("Musicbrainz")) {
            fileEnding = "";
            nullString = "\\N";
            quoteChar = '\0';
        }
        executeHyFD();
    }

    public static List<Result> executeHyFD() {
        List<Result> allResults = new ArrayList<>();
        try {
            String datasetPath = datasetFolder + datasetName;
            File folder = new File(datasetPath);
            File[] files = folder.listFiles();
            files = Arrays.stream(files)
                    .filter(File::isFile)
                    .toArray(File[]::new);
            datasetName = folder.getName();
            relationNames = new String[files.length];
            for (int i = 0; i < files.length; i++) {
                relationNames[i] = files[i].getName().replaceFirst("[.][^.]+$", "");
            }
            RelationalInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[relationNames.length];
            for (int i = 0; i < relationNames.length; i++) {
                File file = new File(datasetFolder + datasetName + File.separator + relationNames[i] + fileEnding);
                fileInputGenerators[i] = getInputGenerator(file);
            }

            long time = System.currentTimeMillis();
            for (RelationalInputGenerator inputGenerator : fileInputGenerators) {
                //if (!inputGenerator.generateNewCopy().relationName().equals("recording"))
                //    continue;
                //System.out.println(inputGenerator.generateNewCopy().relationName());
                ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(inputGenerator));
                HyFD hyFD = createHyFD(threshold, inputGenerator, resultReceiver);
                hyFD.execute();
                //for (Result fd : resultReceiver.fetchNewResults())
                //    System.out.println(fd);
            }
            time = System.currentTimeMillis() - time;
            String content = "Partial HyFD," + datasetName + "," + time + "," + threshold + "\n";
            //FileUtils.writeToFile(content, System.getProperty("java.io.tmpdir") + File.separator + resultFileName, Charset.defaultCharset(), true);
            Files.write(Paths.get(System.getProperty("java.io.tmpdir"), resultFileName), content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
        return allResults;
    }

    public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();
        for (String columnName : relationalInput.columnNames()) {
            acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
        }
        return acceptedColumns;
    }

    public static HyFD createHyFD(double value, RelationalInputGenerator input, ResultCache resultReceiver) throws AlgorithmConfigurationException {
        HyFD hyFD = new HyFD();
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
        hyFD.setStringConfigurationValue(HyFD.Identifier.THRESHOLD.name(), "" + value);//96,80
        hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), -1);
        hyFD.setResultReceiver(resultReceiver);
        return hyFD;
    }

    public static RelationalInputGenerator getInputGenerator(File fileName) throws AlgorithmConfigurationException {
        return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                fileName.getAbsolutePath(),
                true,
                separator,
                quoteChar,
                escapeChar,
                strictQuots,
                true,
                0,
                inputFileHasHeader,
                true,
                nullString
        ));
    }
}
