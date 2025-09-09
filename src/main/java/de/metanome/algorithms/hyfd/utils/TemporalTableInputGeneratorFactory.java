package de.metanome.algorithms.hyfd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.input.FileInputGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TemporalTableInputGeneratorFactory {

    private final File multiTableJsonFile;
    private final ObjectMapper jsonMapper;

    public TemporalTableInputGeneratorFactory(File multiTableJsonFile) {
        this.multiTableJsonFile = multiTableJsonFile;
        this.jsonMapper = new ObjectMapper();
    }

    public List<FileInputGenerator> createGenerators() throws IOException {
        List<FileInputGenerator> generators = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(multiTableJsonFile))) {
            String jsonLine;
            while ((jsonLine = reader.readLine()) != null) {
                if (jsonLine.trim().isEmpty()) {
                    continue;
                }

                JsonNode rootNode = jsonMapper.readTree(jsonLine);
                String pageId = rootNode.get("pageID").asText();
                String tableId = rootNode.get("tableID").asText();
                String uniqueTableName = pageId + "_" + tableId;

                // Pass the entire JSON line (which represents one table's history)
                // to a specialized, in-memory generator.
                generators.add(new InMemoryTemporalTableGenerator(uniqueTableName, jsonLine));
            }
        }
        return generators;
    }
}
