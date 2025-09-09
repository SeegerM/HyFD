package de.metanome.algorithms.hyfd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;

import java.util.List;

public class InMemoryTableInputGenerator implements RelationalInputGenerator {

    private final String tableName;
    private final List<JsonNode> tableRevisions;

    public InMemoryTableInputGenerator(String tableName, List<JsonNode> tableRevisions) {
        this.tableName = tableName;
        this.tableRevisions = tableRevisions;
    }

    @Override
    public RelationalInput generateNewCopy() throws InputGenerationException {
        try {
            return new TemporalInMemoryIterator(this.tableName, this.tableRevisions);
        } catch (InputIterationException e) {
            throw new InputGenerationException("Failed to create in-memory iterator for table " + this.tableName, e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
