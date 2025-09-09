package de.metanome.algorithms.hyfd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import org.jsoup.Jsoup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TemporalInMemoryIterator implements RelationalInput {
    private final String relationName;
    private List<String> nextRow;
    private List<String> columnHeaders;
    private int numberOfColumns = 0;

    private final Iterator<JsonNode> revisionIterator;
    private Iterator<JsonNode> rowIterator;
    private String currentTimestamp;

    /**
     * Constructor takes a list of JSON nodes representing all revisions of a SINGLE table.
     */
    public TemporalInMemoryIterator(String relationName, List<JsonNode> tableRevisions) throws InputIterationException {
        this.relationName = relationName;
        this.revisionIterator = tableRevisions.iterator();

        // Initialize by fetching the first row to set up metadata
        this.nextRow = fetchNextTemporalRow();
        if (this.nextRow != null) {
            // We set the headers from the very first data row we encounter.
            // Note: The timestamp column is the last one.
            this.columnHeaders = new ArrayList<>(this.nextRow.subList(0, this.nextRow.size() - 1));
            this.numberOfColumns = this.columnHeaders.size();
        }
    }

    private List<String> fetchNextTemporalRow() {
        while (true) {
            // If we are iterating through rows of a revision, get the next one.
            if (rowIterator != null && rowIterator.hasNext()) {
                JsonNode rowNode = rowIterator.next();
                List<String> values = StreamSupport.stream(rowNode.spliterator(), false)
                        .map(cell -> extractContentFromCell(cell.get("content").asText()))
                        .collect(Collectors.toList());

                values.add(currentTimestamp);
                return values;
            }

            // If rows are done, move to the next revision.
            if (revisionIterator.hasNext()) {
                JsonNode revisionNode = revisionIterator.next();
                this.currentTimestamp = revisionNode.get("revisionDate").asText();
                JsonNode cellsNode = revisionNode.get("cells");

                if (cellsNode != null && cellsNode.isArray() && cellsNode.size() > 1) {
                    // Skip the header row (index 0) and start iterating data rows.
                    this.rowIterator = StreamSupport.stream(cellsNode.spliterator(), false).skip(1).iterator();
                    continue; // Re-loop to process the first row of this new revision
                }
            } else {
                // No more revisions for this table.
                return null;
            }
        }
    }

    // (Helper methods: extractContentFromCell, hasNext, next, etc. remain the same)
    private String extractContentFromCell(String htmlContent) {
        if (htmlContent == null) return "";
        return Jsoup.parse(htmlContent).text().trim();
    }

    @Override
    public boolean hasNext() {
        return this.nextRow != null;
    }

    @Override
    public List<String> next() throws InputIterationException {
        List<String> currentRow = this.nextRow;
        this.nextRow = fetchNextTemporalRow();
        return currentRow;
    }

    @Override
    public int numberOfColumns() {
        return this.numberOfColumns;
    }

    @Override
    public String relationName() {
        return this.relationName;
    }

    @Override
    public List<String> columnNames() {
        return this.columnHeaders;
    }

    @Override
    public void close() throws IOException { /* Nothing to close for in-memory */ }
}
