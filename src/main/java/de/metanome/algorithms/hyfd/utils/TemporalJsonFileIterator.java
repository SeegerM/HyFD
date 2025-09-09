package de.metanome.algorithms.hyfd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import org.jsoup.Jsoup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class TemporalJsonFileIterator implements RelationalInput {

    private final String relationName;
    private final BufferedReader fileReader;
    private final ObjectMapper jsonMapper;

    // State-keeping variables
    private List<String> nextRow;
    private List<String> columnHeaders;
    private int numberOfColumns = 0;

    private Iterator<JsonNode> revisionIterator;
    private Iterator<JsonNode> rowIterator;
    private String currentTimestamp;

    public TemporalJsonFileIterator(String relationName, Reader reader) throws InputIterationException {
        this.relationName = relationName;
        this.fileReader = new BufferedReader(reader);
        this.jsonMapper = new ObjectMapper();

        // Initialize the first record to set up headers and column count
        this.nextRow = fetchNextTemporalRow();
        if (this.columnHeaders == null && this.nextRow != null) {
            // This is a simplification; you might want a more robust way to find the "best" header
            // across all revisions. For now, we use the header from the first data we encounter.
            this.columnHeaders = this.nextRow.subList(0, this.nextRow.size() - 1);
            this.numberOfColumns = this.columnHeaders.size();
        }
    }

    /**
     * The main logic to fetch the next available row from the nested JSON structure.
     * It handles moving between rows, revisions, and file lines.
     */
    private List<String> fetchNextTemporalRow() throws InputIterationException {
        while (true) {
            // If we have a row iterator and it has rows, process the next one.
            if (rowIterator != null && rowIterator.hasNext()) {
                JsonNode rowNode = rowIterator.next();
                List<String> values = StreamSupport.stream(rowNode.spliterator(), false)
                        .map(cell -> extractContentFromCell(cell.get("content").asText()))
                        .collect(Collectors.toList());

                // Append the timestamp of the current revision
                values.add(currentTimestamp);
                return values;
            }

            // If the row iterator is exhausted, try to move to the next revision.
            if (revisionIterator != null && revisionIterator.hasNext()) {
                JsonNode revisionNode = revisionIterator.next();
                this.currentTimestamp = revisionNode.get("revisionDate").asText();

                // Get the rows from this revision's 'cells'
                JsonNode cellsNode = revisionNode.get("cells");
                if (cellsNode != null && cellsNode.isArray() && cellsNode.size() > 0) {
                    // We assume the first row is the header and skip it
                    this.rowIterator = StreamSupport.stream(cellsNode.spliterator(), false).skip(1).iterator();
                    continue; // Loop again to process the first data row of this new revision
                }
            }

            // If the revision iterator is exhausted, try to read the next line from the file.
            try {
                String line = fileReader.readLine();
                if (line == null) {
                    return null; // End of file
                }
                JsonNode rootNode = jsonMapper.readTree(line);
                JsonNode tablesNode = rootNode.get("tables");
                if (tablesNode != null && tablesNode.isArray()) {
                    this.revisionIterator = tablesNode.iterator();
                    continue; // Loop again to process the first revision of this new table
                }
            } catch (IOException e) {
                throw new InputIterationException("Failed to read or parse JSON line", e);
            }
        }
    }

    /**
     * A helper to clean the cell content by removing HTML tags.
     */
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
        // We do not include the appended timestamp column in this count
        return this.numberOfColumns;
    }

    @Override
    public String relationName() {
        return this.relationName;
    }

    @Override
    public List<String> columnNames() {
        // Return the headers we extracted
        return this.columnHeaders;
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}
