package io.milvus.bulkwriter.writer;

import java.io.IOException;
import java.util.Map;

public interface FormatFileWriter {
    void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException;

    String getFilePath();

    void close() throws IOException;
}
