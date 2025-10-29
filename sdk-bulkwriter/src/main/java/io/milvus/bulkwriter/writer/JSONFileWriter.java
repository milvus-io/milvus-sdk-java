package io.milvus.bulkwriter.writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public class JSONFileWriter implements FormatFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(JSONFileWriter.class);

    private BufferedWriter writer;
    private CreateCollectionReq.CollectionSchema collectionSchema;
    private String filePath;

    public JSONFileWriter(CreateCollectionReq.CollectionSchema collectionSchema, String filePathPrefix) throws IOException {
        this.collectionSchema = collectionSchema;
        initFilePath(filePathPrefix);
        initWriter();
    }

    private void initFilePath(String filePathPrefix) {
        this.filePath = filePathPrefix + ".json";
    }

    private void initWriter() throws IOException {
        this.writer = new BufferedWriter(new java.io.FileWriter(filePath));
    }

    @Override
    public void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException {
        Gson gson = new GsonBuilder().serializeNulls().create();
        rowValues.keySet().removeIf(key -> key.equals(DYNAMIC_FIELD_NAME) && !this.collectionSchema.isEnableDynamicField());
        rowValues.replaceAll((key, value) -> value instanceof ByteBuffer ? ((ByteBuffer) value).array() : value);

        try {
            if (firstWrite) {
                writer.write("[\n");
            } else {
                writer.write(",");
                writer.newLine();
            }
            String json = gson.toJson(rowValues);
            writer.write(json);
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
        appendEnd();
        this.writer.close();
    }

    /**
     * For JSON format data, at the end, it is necessary to append "]" to complete the data.
     */
    private void appendEnd() throws IOException {
        this.writer.newLine();
        this.writer.write("]\n");
    }
}
