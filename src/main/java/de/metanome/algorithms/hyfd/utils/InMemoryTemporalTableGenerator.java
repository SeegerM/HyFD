package de.metanome.algorithms.hyfd.utils;

import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

import java.io.File;

public class InMemoryTemporalTableGenerator implements FileInputGenerator {

    private final String tableName;
    private final String tableJsonData;

    public InMemoryTemporalTableGenerator(String tableName, String tableJsonData) {
        this.tableName = tableName;
        this.tableJsonData = tableJsonData;
    }

    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException {
        try {
            // Create the iterator directly from the in-memory string
            return new InMemoryTemporalTableIterator(tableName, tableJsonData);
        } catch (InputIterationException e) {
            throw new InputGenerationException("Error initializing in-memory temporal iterator", e);
        }
    }

    @Override
    public File getInputFile() {
        // This is a bit of a hack, as there's no real "file" for this one table.
        // We create a virtual File object to satisfy the interface.
        return new File(tableName);
    }

    @Override
    public void close() throws Exception {

    }
}