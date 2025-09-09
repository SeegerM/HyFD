package de.metanome.algorithms.hyfd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InMemoryTemporalTableIterator implements RelationalInput {

    private final String relationName;
    private final ObjectMapper jsonMapper;

    private List<String> nextRow;
    private List<String> columnHeaders;
    private int numberOfColumns = 0;

    private Iterator<JsonNode> revisionIterator;
    private Iterator<JsonNode> rowIterator;
    private String currentTimestamp;

    // *** THE MAIN CHANGE IS HERE: CONSTRUCTOR TAKES JSON STRING ***
    public InMemoryTemporalTableIterator(String relationName, String tableJsonData) throws InputIterationException {
        this.relationName = relationName;
        this.jsonMapper = new ObjectMapper();

        try {
            JsonNode rootNode = jsonMapper.readTree(tableJsonData);
            JsonNode tablesNode = rootNode.get("tables");
            if (tablesNode != null && tablesNode.isArray()) {
                this.revisionIterator = tablesNode.iterator();
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

                values.add(currentTimestamp);
                return values;
            }

            if (revisionIterator != null && revisionIterator.hasNext()) {
                JsonNode revisionNode = revisionIterator.next();
                this.currentTimestamp = revisionNode.get("revisionDate").asText();
                JsonNode cellsNode = revisionNode.get("cells");
                if (cellsNode != null && cellsNode.isArray() && cellsNode.size() > 0) {

                    // Robust header extraction: get header from the first row of cells
                    if (this.columnHeaders == null) {
                        this.columnHeaders = StreamSupport.stream(cellsNode.get(0).spliterator(), false)
                                .map(cell -> extractContentFromCell(cell.path("content").asText()))
                                .collect(Collectors.toList());
                        this.numberOfColumns = this.columnHeaders.size();
                    }

                    this.rowIterator = StreamSupport.stream(cellsNode.spliterator(), false).skip(1).iterator();
                    continue;
                }
            } else {
                // No more revisions to process for this table
                return null;
            }
        }
    }

    // ... (rest of the class is identical to TemporalJsonFileIterator) ...
    // hasNext(), next(), numberOfColumns(), relationName(), columnNames(), close(), extractContentFromCell()
    private String extractContentFromCell(String htmlContent) {
        if (htmlContent == null) return "";
        return Jsoup.parse(htmlContent).text().trim();
    }

    @Override
    public boolean hasNext() { return this.nextRow != null; }

    @Override
    public List<String> next() {
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
    public void close() throws IOException { /* Nothing to close for in-memory */ }
}
