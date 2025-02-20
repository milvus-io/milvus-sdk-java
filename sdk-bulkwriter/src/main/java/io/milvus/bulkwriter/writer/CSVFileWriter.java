package io.milvus.bulkwriter.writer;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.milvus.param.collection.CollectionSchemaParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public class CSVFileWriter implements FormatFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileWriter.class);

    private BufferedWriter writer;
    private CollectionSchemaParam collectionSchema;
    private String filePath;
    private Map<String, Object> config;

    public CSVFileWriter(CollectionSchemaParam collectionSchema, String filePathPrefix, Map<String, Object> config) throws IOException {
        this.collectionSchema = collectionSchema;
        this.config = config;
        initFilePath(filePathPrefix);
        initWriter();
    }

    private void initFilePath(String filePathPrefix) {
        this.filePath = filePathPrefix +  ".csv";
    }

    private void initWriter() throws IOException {
        this.writer = new BufferedWriter(new java.io.FileWriter(filePath));
    }

    @Override
    public void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException {
        rowValues.keySet().removeIf(key -> key.equals(DYNAMIC_FIELD_NAME) && !this.collectionSchema.isEnableDynamicField());

        Gson gson = new GsonBuilder().serializeNulls().create();
        List<String> fieldNameList = Lists.newArrayList(rowValues.keySet());

        try {
            String separator = (String)config.getOrDefault("sep", "\t");
            String nullKey = (String)config.getOrDefault("nullkey", "");

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
                } else if (val instanceof ByteBuffer) {
                    strVal = Arrays.toString(((ByteBuffer) val).array());
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
                strVal = "\"" + strVal + "\"";
                values.add(strVal);
            }

            writer.write(String.join(separator, values));
            writer.newLine();
        } catch (IOException e) {
            logger.error("{} appendRow error when writing to file {}", this.getClass().getSimpleName(), filePath, e);
            throw e;
        }
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
