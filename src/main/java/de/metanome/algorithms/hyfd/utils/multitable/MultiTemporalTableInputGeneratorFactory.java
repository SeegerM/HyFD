package de.metanome.algorithms.hyfd.utils.multitable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.metanome.algorithm_integration.input.FileInputGenerator;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Factory that can create one FileInputGenerator per *table* from either:
 * - a single JSON-line string (which may describe one table or many tables on a page), or
 * - a file containing multiple JSON lines.
 *
 * It supports two JSON shapes:
 * (A) One-table-per-line (your current format):
 * { "pageID":"p1", "tableID":"t1", "tables":[ {revision...}, ... ] }
 *
 * (B) Many-tables-in-one-line (page-level):
 * { "pageID":"p1", "tables": [
 * { "tableID":"t1", "tables":[ {revision...}, ... ] },
 * { "tableID":"t2", "tables":[ ... ] }
 * ]}
 */
public class MultiTemporalTableInputGeneratorFactory {

    private final String jsonLine;
    private final ObjectMapper jsonMapper = new ObjectMapper();


    public MultiTemporalTableInputGeneratorFactory(String multiTableJsonString) {
        this.jsonLine = multiTableJsonString;
    }


    /**
     * Convenience: read a file of JSONL where each line is either a table object (A)
     * or a page object containing many tables (B). Returns one generator per table found.
     */
    public List<FileInputGenerator> createGenerators() throws IOException {
        return createGeneratorsFromJsonLine(jsonLine);
    }


    /**
     * Parse a single JSON line that could define one table or multiple tables.
     * Returns one generator per *table* discovered.
     */
    public List<FileInputGenerator> createGeneratorsFromJsonLine(String jsonLine) throws IOException {
        List<FileInputGenerator> generators = new ArrayList<>();


        JsonNode root = jsonMapper.readTree(jsonLine);
        String pageId = getTextOr(root, "pageID", "page");

// Case A: one table on root with a revisions array -> make ONE GENERATOR PER REVISION
        if (root.hasNonNull("tableID") && isRevisionsArray(root.get("tables"))) {
            String tableId = getTextOr(root, "tableID", "unknown");
            JsonNode revisions = root.get("tables");
            int idx = 0;
            for (JsonNode rev : revisions) {
// Build a per-revision table object that keeps pageID/tableID but contains only this single revision
                ObjectNode tableObj = jsonMapper.createObjectNode();
                if (pageId != null) tableObj.put("pageID", pageId);
                tableObj.put("tableID", tableId);
                tableObj.set("tables", jsonMapper.createArrayNode().add(rev));


                String revId = getTextOr(rev, "revisionID", String.valueOf(idx));
                String uniqueTableName = (pageId != null ? pageId : "page") + "_" + tableId;//+ "_rev_" + revId;


                String isolatedTableJson = jsonMapper.writeValueAsString(tableObj);
                generators.add(new TemporalTableGenerator(uniqueTableName, isolatedTableJson));
                idx++;
            }
            return generators;
        }


// Case B: page-level object containing multiple table objects -> split by table, THEN by revision
        if (isTablesArray(root.get("tables"))) {
            for (JsonNode tableNode : root.get("tables")) {
                if (!tableNode.isObject()) continue;
                ObjectNode tableObj = (ObjectNode) tableNode.deepCopy();


                String tableId = getTextOr(tableObj, "tableID", "unknown");
                if (!tableObj.has("pageID") && pageId != null) tableObj.put("pageID", pageId);


                JsonNode revisions = tableObj.get("tables");
                if (!isRevisionsArray(revisions)) continue; // defensive


                int idx = 0;
                for (JsonNode rev : revisions) {
                    ObjectNode single = jsonMapper.createObjectNode();
                    single.put("pageID", getTextOr(tableObj, "pageID", pageId));
                    single.put("tableID", tableId);
                    single.set("tables", jsonMapper.createArrayNode().add(rev));


                    String revId = getTextOr(rev, "revisionID", String.valueOf(idx));
                    String uniqueTableName = (pageId != null ? pageId : "page") + "_" + tableId + "_rev_" + revId;


                    String isolated = jsonMapper.writeValueAsString(single);
                    generators.add(new TemporalTableGenerator(uniqueTableName, isolated));
                    idx++;
                }
            }
            return generators;
        }


// Fallback: treat the whole object as one table with a synthetic ID
        String uniqueTableName = (pageId != null ? pageId : "page") + "_t0_rev_0";
        generators.add(new TemporalTableGenerator(uniqueTableName, jsonLine));
        return generators;
    }


    private static boolean isRevisionsArray(JsonNode tablesNode) {
        if (tablesNode == null || !tablesNode.isArray() || tablesNode.isEmpty()) return false;
        JsonNode first = tablesNode.get(0);
        return first.has("cells");
    }


    private static boolean isTablesArray(JsonNode tablesNode) {
        if (tablesNode == null || !tablesNode.isArray() || tablesNode.isEmpty()) return false;
        JsonNode first = tablesNode.get(0);
        return first.has("tableID");
    }

    private static String getTextOr(JsonNode n, String key, String fallback) {
        JsonNode v = n.get(key);
        return v != null && !v.isNull() ? v.asText() : fallback;
    }
}