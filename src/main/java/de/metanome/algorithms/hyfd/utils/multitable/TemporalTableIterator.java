package de.metanome.algorithms.hyfd.utils.multitable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * RelationalInput that iterates over the rows of a temporal table across revisions.
 *
 * Expected per-table JSON shape:
 * {
 *   "pageID": "p1",
 *   "tableID": "t1",
 *   "tables": [
 *     { "revisionDate": "2024-05-01T00:00:00Z", "cells": [ [ {content:"..."}, ... ], ... ] },
 *     ...
 *   ]
 * }
 */
public class TemporalTableIterator implements RelationalInput {

    private final String relationName;
    private final ObjectMapper jsonMapper;

    private List<String> nextRow;
    private List<String> columnHeaders;
    private int numberOfColumns = 0;

    private Iterator<JsonNode> revisionIterator;
    private Iterator<JsonNode> rowIterator;
    private String currentTimestamp;
    private int currentTimeIndex = -1; // start at -1, increment when we load a revision

    private final boolean appendTimestamp; // if true, append revisionDate; otherwise append index
    private int rowCount = 0;

    public TemporalTableIterator(String relationName, String tableJsonData) throws InputIterationException {
        this(relationName, tableJsonData, false);
    }

    public TemporalTableIterator(String relationName, String tableJsonData, boolean appendTimestamp) throws InputIterationException {
        this.relationName = relationName;
        this.appendTimestamp = appendTimestamp;
        this.jsonMapper = new ObjectMapper();

        try {
            JsonNode rootNode = jsonMapper.readTree(tableJsonData);
            JsonNode tablesNode = rootNode.get("tables");
            if (tablesNode != null && tablesNode.isArray()) {
                this.revisionIterator = tablesNode.iterator();
            } else {
                throw new InputIterationException("Missing or non-array 'tables' on table JSON for: " + relationName);
            }
        } catch (IOException e) {
            throw new InputIterationException("Failed to parse table JSON data", e);
        }

        // Initialize by fetching the first row
        this.nextRow = fetchNextTemporalRow();
    }

    private List<String> fetchNextTemporalRow() throws InputIterationException {
        while (true) {
            if (rowIterator != null && rowIterator.hasNext()) {
                JsonNode rowNode = rowIterator.next();
                List<String> values = StreamSupport.stream(rowNode.spliterator(), false)
                        .map(cell -> extractContentFromCell(cell.path("content").asText()))
                        .collect(Collectors.toList());

                // Backfill if the row is shorter than the header width
                if (columnHeaders != null && values.size() < columnHeaders.size() - 1) {
                    int toAdd = (columnHeaders.size() - 1) - values.size();
                    for (int i = 0; i < toAdd; i++) values.add("");
                }
                if (appendTimestamp)
                    values.add(currentTimestamp != null ? currentTimestamp : String.valueOf(currentTimeIndex));
                return values;
            }

            if (revisionIterator != null && revisionIterator.hasNext()) {
                JsonNode revisionNode = revisionIterator.next();
                this.currentTimestamp = getTextOr(revisionNode, "revisionDate", null);
                this.currentTimeIndex++;

                JsonNode cellsNode = revisionNode.get("cells");
                if (cellsNode == null || !cellsNode.isArray() || cellsNode.isEmpty()) {
                    // Empty revision: continue to next revision
                    continue;
                }

                // Initialize headers once from the first row of the first non-empty revision
                if (this.columnHeaders == null) {
                    List<String> rawHeaders = StreamSupport.stream(cellsNode.get(0).spliterator(), false)
                            .map(cell -> extractContentFromCell(cell.path("content").asText()))
                            .collect(Collectors.toList());

                    if (rawHeaders.stream().allMatch(String::isEmpty)) {
                        rawHeaders = autoHeaders(rawHeaders.size());
                    }

                    this.columnHeaders = uniquifyHeaders(rawHeaders);
                    if (appendTimestamp)
                        this.columnHeaders.add("Time");
                    this.numberOfColumns = this.columnHeaders.size();
                }

                // Iterate over data rows (skip header row at index 0)
                this.rowIterator = StreamSupport.stream(cellsNode.spliterator(), false).skip(1).iterator();
                continue; // loop will try rowIterator above
            } else {
                // No more revisions
                return null;
            }
        }
    }

    private static List<String> autoHeaders(int n) {
        List<String> headers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) headers.add("column_" + i);
        return headers;
    }

    /**
     * Make headers unique while preserving order.
     */
    private static List<String> uniquifyHeaders(List<String> headers) {
        Map<String, Integer> seen = new LinkedHashMap<>();
        List<String> out = new ArrayList<>(headers.size());
        for (String h : headers) {
            if (h == null) h = "";
            String base = h;
            Integer cnt = seen.get(base);
            if (cnt == null) {
                seen.put(base, 1);
                out.add(base);
            } else {
                String candidate;
                int i = cnt;
                do {
                    candidate = base + "_" + i;
                    i++;
                } while (seen.containsKey(candidate));
                seen.put(base, i);
                seen.put(candidate, 1);
                out.add(candidate);
            }
        }
        return out;
    }

    private static String getTextOr(JsonNode n, String key, String fallback) {
        JsonNode v = n.get(key);
        return v != null && !v.isNull() ? v.asText() : fallback;
    }

    private static String extractContentFromCell(String htmlContent) {
        if (htmlContent == null) return "";
        return Jsoup.parse(htmlContent).text().trim();
    }

    @Override
    public boolean hasNext() { return this.nextRow != null; }

    @Override
    public List<String> next() {
        rowCount++;
        List<String> currentRow = this.nextRow;
        try {
            this.nextRow = fetchNextTemporalRow();
        } catch (InputIterationException e) {
            throw new RuntimeException(e);
        }
        return currentRow;
    }

    @Override
    public int numberOfColumns() { return this.numberOfColumns; }

    @Override
    public String relationName() { return this.relationName; }

    @Override
    public List<String> columnNames() { return this.columnHeaders; }

    @Override
    public void close() throws IOException {}
}
