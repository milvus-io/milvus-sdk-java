package io.milvus.bulkwriter.writer;

import io.milvus.bulkwriter.common.utils.ParquetUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

/**
 * {@link FormatFileWriter} implementation that writes bulk-import data rows to a Parquet data file.
 *
 * <p>The writer maps each field of the collection schema to the corresponding Parquet type and
 * appends array, vector, and struct fields as nested Parquet groups. Data rows are buffered into
 * row groups and flushed to the file on {@link #close()}.
 */
public class ParquetFileWriter implements FormatFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetFileWriter.class);

    private ParquetWriter<Group> writer;
    private CreateCollectionReq.CollectionSchema collectionSchema;
    private String filePath;
    private MessageType messageType;
    private Map<String, CreateCollectionReq.FieldSchema> nameFieldType;
    private Map<String, CreateCollectionReq.StructFieldSchema> nameStructFieldType;

    /**
     * Creates a Parquet data file writer.
     *
     * @param collectionSchema the collection schema describing the fields to be written
     * @param filePathPrefix the file path prefix; the {@code .parquet} extension is appended to
     *                       form the data file path
     * @throws IOException if the Parquet data file cannot be created
     */
    public ParquetFileWriter(CreateCollectionReq.CollectionSchema collectionSchema, String filePathPrefix) throws IOException {
        this.collectionSchema = collectionSchema;
        initFilePath(filePathPrefix);
        initNameFieldType();
        initMessageType();
        initWriter();
    }

    private void initFilePath(String filePathPrefix) {
        this.filePath = filePathPrefix + ".parquet";
    }

    private void initMessageType() {
        // declare the messageType of the Parquet
        this.messageType = ParquetUtils.parseCollectionSchema(collectionSchema);
    }

    private void initWriter() throws IOException {
        int rowGroupBytes = 16 * 1024 * 1024;

        // declare and define the ParquetWriter.
        Configuration configuration = ParquetUtils.getParquetConfiguration();
        GroupWriteSupport.setSchema(messageType, configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        this.writer = new ParquetWriter<>(new Path(filePath),
                org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE,
                writeSupport,
                CompressionCodecName.UNCOMPRESSED,
                rowGroupBytes,
                2 * 1024 * 1024,
                2 * 1024 * 1024,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetWriter.DEFAULT_WRITER_VERSION,
                configuration);
    }

    private void initNameFieldType() {
        Map<String, CreateCollectionReq.FieldSchema> nameFieldType = collectionSchema.getFieldSchemaList().stream()
                .collect(Collectors.toMap(CreateCollectionReq.FieldSchema::getName, e -> e));
        if (collectionSchema.isEnableDynamicField()) {
            nameFieldType.put(DYNAMIC_FIELD_NAME, CreateCollectionReq.FieldSchema.builder()
                    .name(DYNAMIC_FIELD_NAME)
                    .dataType(io.milvus.v2.common.DataType.JSON)
                    .build());
        }
        this.nameFieldType = nameFieldType;

        this.nameStructFieldType = collectionSchema.getStructFields().stream()
                .collect(Collectors.toMap(CreateCollectionReq.StructFieldSchema::getName, e -> e));
    }

    /**
     * Appends a row of values to the Parquet data file.
     *
     * @param rowValues the row values keyed by field name
     * @param firstWrite {@code true} if this is the first row written to the data file chunk,
     *                   {@code false} otherwise; ignored for Parquet since no header row is needed
     * @throws IOException if the row cannot be written to the Parquet file
     */
    @Override
    public void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException {
        rowValues.keySet().removeIf(key -> key.equals(DYNAMIC_FIELD_NAME) && !this.collectionSchema.isEnableDynamicField());

        try {
            Group group = new SimpleGroupFactory(messageType).newGroup();
            for (String fieldName : rowValues.keySet()) {
                Object value = rowValues.get(fieldName);
                if (value == null) {
                    continue;
                }
                if (nameFieldType.containsKey(fieldName)) {
                    appendGroup(group, value, nameFieldType.get(fieldName));
                } else if (nameStructFieldType.containsKey(fieldName)) {
                    appendStructGroup(group, value, nameStructFieldType.get(fieldName));
                }
            }
            writer.write(group);
        } catch (IOException e) {
            logger.error("{} appendRow error when writing to file {}", this.getClass().getSimpleName(), filePath, e);
            throw e;
        }
    }

    /**
     * Returns the file path of the Parquet data file being written.
     *
     * @return the file path of the Parquet data file
     */
    @Override
    public String getFilePath() {
        return filePath;
    }

    /**
     * Closes the Parquet data file, flushing any buffered rows and releasing resources.
     *
     * @throws IOException if the Parquet data file cannot be closed
     */
    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    private void appendGroup(Group group, Object value, CreateCollectionReq.FieldSchema field) {
        io.milvus.v2.common.DataType dataType = field.getDataType();
        String fieldName = field.getName();
        switch (dataType) {
            case Int8:
            case Int16:
                group.append(fieldName, (Short) value);
                break;
            case Int32:
                group.append(fieldName, (Integer) value);
                break;
            case Int64:
                group.append(fieldName, (Long) value);
                break;
            case Float:
                group.append(fieldName, (Float) value);
                break;
            case Double:
                group.append(fieldName, (Double) value);
                break;
            case Bool:
                group.append(fieldName, (Boolean) value);
                break;
            case VarChar:
            case String:
            case Text:
            case Geometry:
            case Timestamptz:
            case JSON:
                group.append(fieldName, (String) value);
                break;
            case FloatVector:
                addFloatArray(group, fieldName, (List<Float>) value);
                break;
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector:
            case Int8Vector:
                addBinaryVector(group, fieldName, (ByteBuffer) value);
                break;
            case SparseFloatVector:
                addSparseVector(group, fieldName, (SortedMap<Long, Float>) value);
                break;
            case Array:
                io.milvus.v2.common.DataType elementType = field.getElementType();
                switch (elementType) {
                    case Int8:
                    case Int16:
                    case Int32:
                        addIntArray(group, fieldName, (List<Integer>) value);
                        break;
                    case Int64:
                        addLongArray(group, fieldName, (List<Long>) value);
                        break;
                    case Float:
                        addFloatArray(group, fieldName, (List<Float>) value);
                        break;
                    case Double:
                        addDoubleArray(group, fieldName, (List<Double>) value);
                        break;
                    case String:
                    case VarChar:
                        addStringArray(group, fieldName, (List<String>) value);
                        break;
                    case Bool:
                        addBooleanArray(group, fieldName, (List<Boolean>) value);
                        break;
                }
        }
    }

    private void appendStructGroup(Group group, Object value, CreateCollectionReq.StructFieldSchema field) {
        Group arrayGroup = group.addGroup(field.getName());
        Group listGroup = arrayGroup.addGroup(0);
        List<Map<String, Object>> structs = (List<Map<String, Object>>) value;
        for (Map<String, Object> struct : structs) {
            Group dict = listGroup.addGroup(0);
            for (CreateCollectionReq.FieldSchema subField : field.getFields()) {
                if (struct.containsKey(subField.getName())) {
                    Object val = struct.get(subField.getName());
                    appendGroup(dict, val, subField);
                }
            }
        }
    }

    private static void addLongArray(Group group, String fieldName, List<Long> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (long value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addStringArray(Group group, String fieldName, List<String> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (String value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addIntArray(Group group, String fieldName, List<Integer> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (int value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addFloatArray(Group group, String fieldName, List<Float> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (float value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addDoubleArray(Group group, String fieldName, List<Double> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (double value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addBooleanArray(Group group, String fieldName, List<Boolean> values) {
        Group arrayGroup = group.addGroup(fieldName);
        for (boolean value : values) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addBinaryVector(Group group, String fieldName, ByteBuffer byteBuffer) {
        Group arrayGroup = group.addGroup(fieldName);
        byte[] bytes = byteBuffer.array();
        for (byte value : bytes) {
            Group addGroup = arrayGroup.addGroup(0);
            addGroup.add(0, value);
        }
    }

    private static void addSparseVector(Group group, String fieldName, SortedMap<Long, Float> sparse) {
        // sparse vector is parsed as JSON format string in the server side
        String jsonString = JsonUtils.toJson(sparse);
        group.append(fieldName, jsonString);
    }
}
