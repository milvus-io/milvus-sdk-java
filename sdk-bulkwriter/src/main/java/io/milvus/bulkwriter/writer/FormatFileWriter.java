package io.milvus.bulkwriter.writer;

import java.io.IOException;
import java.util.Map;

/**
 * Defines the contract for writing bulk-import data files in a specific format.
 *
 * <p>Implementations write the rows of a bulk import job to a local data file (for example, CSV or
 * Parquet) that is later uploaded to remote/cloud storage. The {@code firstWrite} flag passed to
 * {@link #appendRow(Map, boolean)} indicates the first row of a new data file chunk, allowing
 * format-specific headers to be emitted once before the first row is flushed.
 */
public interface FormatFileWriter {
    /**
     * Appends a row of values to the data file.
     *
     * @param rowValues the row values keyed by field name
     * @param firstWrite {@code true} if this is the first row written to a new data file chunk,
     *                   {@code false} otherwise
     * @throws IOException if the row cannot be written to the file
     */
    void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException;

    /**
     * Returns the file path of the data file being written.
     *
     * @return the file path of the data file
     */
    String getFilePath();

    /**
     * Closes the underlying writer and releases any resources held by the data file.
     *
     * @throws IOException if the file cannot be closed
     */
    void close() throws IOException;
}
