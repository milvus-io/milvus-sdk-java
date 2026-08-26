package io.milvus.bulkwriter.writer;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.milvus.bulkwriter.common.utils.WriterUtils;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

/**
 * {@link FormatFileWriter} implementation that writes bulk-import data rows to a CSV data file.
 *
 * <p>The writer produces a CSV file with a header row (emitted on the first row of each chunk),
 * quotes string values and serializes {@link java.util.List} and {@link java.util.Map} values as
 * JSON. Separator and null placeholder values are taken from the supplied configuration.
 */
public class CSVFileWriter implements FormatFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileWriter.class);

    private BufferedWriter writer;
    private CreateCollectionReq.CollectionSchema collectionSchema;
    private String filePath;
    private Map<String, Object> config;

    /**
     * Creates a CSV data file writer.
     *
     * @param collectionSchema the collection schema describing the fields to be written
     * @param filePathPrefix the file path prefix; the CSV extension is appended to form the data
     *                       file path
     * @param config the writer configuration, supporting {@code sep} (column separator) and
     *               {@code nullkey} (placeholder for null values)
     * @throws IOException if the CSV data file cannot be created
     */
    public CSVFileWriter(CreateCollectionReq.CollectionSchema collectionSchema, String filePathPrefix, Map<String, Object> config) throws IOException {
        this.collectionSchema = collectionSchema;
        this.config = config;
        initFilePath(filePathPrefix);
        initWriter();
    }

    private void initFilePath(String filePathPrefix) {
        this.filePath = filePathPrefix + ".csv";
    }

    private void initWriter() throws IOException {
        this.writer = new BufferedWriter(new java.io.FileWriter(filePath));
    }

    /**
     * Appends a row of values to the CSV data file.
     *
     * @param rowValues the row values keyed by field name
     * @param firstWrite {@code true} if this is the first row written to the data file chunk, in
     *                   which case the header row is emitted
     * @throws IOException if the row cannot be written to the CSV file
     */
    @Override
    public void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException {
        rowValues.keySet().removeIf(key -> key.equals(DYNAMIC_FIELD_NAME) && !this.collectionSchema.isEnableDynamicField());
        rowValues.replaceAll((key, value) -> WriterUtils.normalizeValue(value));

        Gson gson = new GsonBuilder().serializeNulls().create();
        List<String> fieldNameList = Lists.newArrayList(rowValues.keySet());

        try {
            String separator = (String) config.getOrDefault("sep", ",");
            String nullKey = (String) config.getOrDefault("nullkey", "");

            if (firstWrite) {
                writer.write(String.join(separator, fieldNameList));
                writer.newLine();
            }

            List<String> values = new ArrayList<>();
            for (String fieldName : fieldNameList) {
                Object val = rowValues.get(fieldName);
                String strVal = "";
                if (val == null) {
                    strVal = nullKey;
                } else if (val instanceof List || val instanceof Map) {
                    strVal = gson.toJson(val); // server-side is using json to parse array field and vector field
                } else {
                    strVal = val.toString();
                }

                // CSV format, all the single quotation should be replaced by double quotation
                if (strVal.startsWith("\"") && strVal.endsWith("\"")) {
                    strVal = strVal.substring(1, strVal.length() - 1);
                }
                strVal = strVal.replace("\\\"", "\"");
                strVal = strVal.replace("\"", "\"\"");
                if (!strVal.isEmpty()) {
                    // some fields might be nullable, the strVal is empty, no need to add ""
                    strVal = "\"" + strVal + "\"";
                }
                values.add(strVal);
            }

            writer.write(String.join(separator, values));
            writer.newLine();
        } catch (IOException e) {
            logger.error("{} appendRow error when writing to file {}", this.getClass().getSimpleName(), filePath, e);
            throw e;
        }
    }

    /**
     * Returns the file path of the CSV data file being written.
     *
     * @return the file path of the CSV data file
     */
    @Override
    public String getFilePath() {
        return filePath;
    }

    /**
     * Closes the CSV data file and releases the underlying writer resources.
     *
     * @throws IOException if the CSV data file cannot be closed
     */
    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
