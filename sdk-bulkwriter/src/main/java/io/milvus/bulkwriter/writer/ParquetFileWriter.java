package io.milvus.bulkwriter.writer;

import io.milvus.bulkwriter.common.utils.ParquetUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
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

public class ParquetFileWriter implements FormatFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetFileWriter.class);

    private ParquetWriter<Group> writer;
    private CollectionSchemaParam collectionSchema;
    private String filePath;
    private MessageType messageType;
    private Map<String, FieldType> nameFieldType;

    public ParquetFileWriter(CollectionSchemaParam collectionSchema, String filePathPrefix) throws IOException {
        this.collectionSchema = collectionSchema;
        initFilePath(filePathPrefix);
        initNameFieldType();
        initMessageType();
        initWriter();
    }

    private void initFilePath(String filePathPrefix) {
        this.filePath = filePathPrefix +  ".parquet";
    }

    private void initMessageType() {
        // declare the messageType of the Parquet
        this.messageType = ParquetUtils.parseCollectionSchema(collectionSchema);
    }

    private void initWriter() throws IOException {
        int rowGroupBytes = 16 * 1024 * 1024;

        // declare and define the ParquetWriter.
        Configuration configuration = new Configuration();
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
        Map<String, FieldType> nameFieldType = collectionSchema.getFieldTypes().stream().collect(Collectors.toMap(FieldType::getName, e -> e));
        if (collectionSchema.isEnableDynamicField()) {
            nameFieldType.put(DYNAMIC_FIELD_NAME, FieldType.newBuilder()
                    .withName(DYNAMIC_FIELD_NAME)
                    .withDataType(DataType.JSON)
                    .build());
        }
        this.nameFieldType = nameFieldType;
    }

    @Override
    public void appendRow(Map<String, Object> rowValues, boolean firstWrite) throws IOException {
        rowValues.keySet().removeIf(key -> key.equals(DYNAMIC_FIELD_NAME) && !this.collectionSchema.isEnableDynamicField());

        try {
            Group group = new SimpleGroupFactory(messageType).newGroup();
            for (String fieldName : rowValues.keySet()) {
                appendGroup(group, fieldName, rowValues.get(fieldName), nameFieldType.get(fieldName));
            }
            writer.write(group);
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

    private void appendGroup(Group group, String paramName, Object value, FieldType fieldType) {
        DataType dataType = fieldType.getDataType();
        switch (dataType) {
            case Int8:
            case Int16:
                group.append(paramName, (Short)value);
                break;
            case Int32:
                group.append(paramName, (Integer)value);
                break;
            case Int64:
                group.append(paramName, (Long)value);
                break;
            case Float:
                group.append(paramName, (Float)value);
                break;
            case Double:
                group.append(paramName, (Double)value);
                break;
            case Bool:
                group.append(paramName, (Boolean)value);
                break;
            case VarChar:
            case String:
            case JSON:
                group.append(paramName, (String)value);
                break;
            case FloatVector:
                addFloatArray(group, paramName, (List<Float>) value);
                break;
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector:
                addBinaryVector(group, paramName, (ByteBuffer) value);
                break;
            case SparseFloatVector:
                addSparseVector(group, paramName, (SortedMap<Long, Float>) value);
                break;
            case Array:
                DataType elementType = fieldType.getElementType();
                switch (elementType) {
                    case Int8:
                    case Int16:
                    case Int32:
                        addIntArray(group, paramName, (List<Integer>) value);
                        break;
                    case Int64:
                        addLongArray(group, paramName, (List<Long>) value);
                        break;
                    case Float:
                        addFloatArray(group, paramName, (List<Float>) value);
                        break;
                    case Double:
                        addDoubleArray(group, paramName, (List<Double>) value);
                        break;
                    case String:
                    case VarChar:
                        addStringArray(group, paramName, (List<String>) value);
                        break;
                    case Bool:
                        addBooleanArray(group, paramName, (List<Boolean>) value);
                        break;
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
