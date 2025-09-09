import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithms.hyfd.utils.TemporalJsonFileInputGenerator;
import de.metanome.algorithms.hyfd.utils.TemporalTableInputGeneratorFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

}
