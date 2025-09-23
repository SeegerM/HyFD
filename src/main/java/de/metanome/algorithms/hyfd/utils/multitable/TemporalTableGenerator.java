package de.metanome.algorithms.hyfd.utils.multitable;

import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;


import java.io.File;


/**
 * Minimal in-memory FileInputGenerator.
 * getInputFile() returns a synthetic File to satisfy the Metanome interface.
 */
public class TemporalTableGenerator implements FileInputGenerator {

    private final String tableName;
    private final String tableJsonData;


    public TemporalTableGenerator(String tableName, String tableJsonData) {
        this.tableName = tableName;
        this.tableJsonData = tableJsonData;
    }


    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException {
        try {
// Create the iterator directly from the in-memory JSON string
            return new TemporalTableIterator(tableName, tableJsonData);
        } catch (InputIterationException e) {
            throw new InputGenerationException("Error initializing in-memory temporal iterator", e);
        }
    }


    @Override
    public File getInputFile() {
// Synthetic file handle; not used by the iterator.
        return new File(tableName + ".json");
    }


    @Override
    public void close() { /* nothing to close */ }
}
