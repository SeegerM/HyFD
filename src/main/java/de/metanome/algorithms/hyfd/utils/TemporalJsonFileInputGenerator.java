package de.metanome.algorithms.hyfd.utils;

import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class TemporalJsonFileInputGenerator implements FileInputGenerator {

    private final File inputFile;

    public TemporalJsonFileInputGenerator(File inputFile) throws FileNotFoundException {
        if (!inputFile.isFile()) {
            throw new FileNotFoundException();
        }
        this.inputFile = inputFile;
    }

    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException {
        try {
            return new TemporalJsonFileIterator(inputFile.getName(), new FileReader(inputFile));
        } catch (FileNotFoundException e) {
            throw new InputGenerationException("Input file not found", e);
        } catch (InputIterationException e) {
            throw new InputGenerationException("Error initializing temporal iterator", e);
        }
    }

    @Override
    public File getInputFile() {
        return this.inputFile;
    }

    @Override
    public void close() throws Exception {
    }
}
