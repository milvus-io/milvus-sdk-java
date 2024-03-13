package io.milvus.bulkwriter;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.bulkwriter.common.utils.ParquetUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public class Buffer {
    private static final Logger logger = LoggerFactory.getLogger(Buffer.class);

    private CollectionSchemaParam collectionSchema;
    private BulkFileType fileType;
    private Map<String, List<Object>> buffer;
    private Map<String, FieldType> fields;

    public Buffer(CollectionSchemaParam collectionSchema, BulkFileType fileType) {
        this.collectionSchema = collectionSchema;
        this.fileType = fileType;

        buffer = new HashMap<>();
        fields = new HashMap<>();

        for (FieldType fieldType : collectionSchema.getFieldTypes()) {
            if (fieldType.isPrimaryKey() && fieldType.isAutoID())
                continue;
            buffer.put(fieldType.getName(), Lists.newArrayList());
            fields.put(fieldType.getName(), fieldType);
        }

        if (buffer.isEmpty()) {
            ExceptionUtils.throwUnExpectedException("Illegal collection schema: fields list is empty");
        }

        if (collectionSchema.isEnableDynamicField()) {
            buffer.put(DYNAMIC_FIELD_NAME, Lists.newArrayList());
            fields.put(DYNAMIC_FIELD_NAME, FieldType.newBuilder().withName(DYNAMIC_FIELD_NAME).withDataType(DataType.JSON).build());
        }
    }

    public Integer getRowCount() {
        if (buffer.isEmpty()) {
            return 0;
        }

        for (String fieldName : buffer.keySet()) {
            return buffer.get(fieldName).size();
        }
        return null;
    }

    public void appendRow(JSONObject row) {
        Map<String, Object> dynamicValues = new HashMap<>();
        if (row.containsKey(DYNAMIC_FIELD_NAME) && !(row.get(DYNAMIC_FIELD_NAME) instanceof Map)) {
            String msg = String.format("Dynamic field '%s' value should be JSON format", DYNAMIC_FIELD_NAME);
            ExceptionUtils.throwUnExpectedException(msg);
        }

        for (String key : row.keySet()) {
            if (DYNAMIC_FIELD_NAME.equals(key)) {
                dynamicValues.putAll((Map<String, Object>) row.get(key));
                continue;
            }
            if (!buffer.containsKey(key)) {
                dynamicValues.put(key, row.get(key));
            } else {
                buffer.get(key).add(row.get(key));
            }
        }

        if (buffer.containsKey(DYNAMIC_FIELD_NAME)) {
            buffer.get(DYNAMIC_FIELD_NAME).add(dynamicValues);
        }
    }

    // verify row count of fields are equal
    public List<String> persist(String localPath, Integer bufferSize, Integer bufferRowCount) {
        int rowCount = -1;
        for (String key : buffer.keySet()) {
            if (rowCount < 0) {
                rowCount = buffer.get(key).size();
            } else if (rowCount != buffer.get(key).size()) {
                String msg = String.format("Column `%s` row count %s doesn't equal to the first column row count %s", key, buffer.get(key).size(), rowCount);
                ExceptionUtils.throwUnExpectedException(msg);
            }
        }

        // output files
        if (fileType == BulkFileType.PARQUET) {
            return persistParquet(localPath, bufferSize, bufferRowCount);
        }
        ExceptionUtils.throwUnExpectedException("Unsupported file type: " + fileType);
        return null;
    }

    private List<String> persistParquet(String localPath, Integer bufferSize, Integer bufferRowCount) {
        String filePath = localPath + ".parquet";

        // calculate a proper row group size
        int rowGroupSizeMin = 1000;
        int rowGroupSizeMax = 1000000;
        int rowGroupSize = 10000;

        // 32MB is an experience value that avoid high memory usage of parquet reader on server-side
        int rowGroupBytes = 32 * 1024 * 1024;

        int sizePerRow = (bufferSize / bufferRowCount) + 1;
        rowGroupSize = rowGroupBytes / sizePerRow;
        rowGroupSize = Math.max(rowGroupSizeMin, Math.min(rowGroupSizeMax, rowGroupSize));

        // declare the messageType of the Parquet
        MessageType messageType = ParquetUtils.parseCollectionSchema(collectionSchema);

        // declare and define the ParquetWriter.
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        try (ParquetWriter<Group> writer = new ParquetWriter<>(path,
                ParquetFileWriter.Mode.CREATE,
                writeSupport,
                CompressionCodecName.UNCOMPRESSED,
                rowGroupBytes,
                5 * 1024 * 1024,
                5 * 1024 * 1024,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetWriter.DEFAULT_WRITER_VERSION,
                configuration)) {

            Map<String, FieldType> nameFieldType = collectionSchema.getFieldTypes().stream().collect(Collectors.toMap(FieldType::getName, e -> e));

            List<String> fieldNameList = Lists.newArrayList(buffer.keySet());
            int size = buffer.get(fieldNameList.get(0)).size();
            for (int i = 0; i < size; ++i) {
                // build Parquet data and encapsulate it into a group.
                Group group = new SimpleGroupFactory(messageType).newGroup();
                for (String fieldName : fieldNameList) {
                    appendGroup(group, fieldName, buffer.get(fieldName).get(i), nameFieldType.get(fieldName));
                }
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String msg = String.format("Successfully persist file %s, total size: %s, row count: %s, row group size: %s",
                filePath, bufferSize, bufferRowCount, rowGroupSize);
        logger.info(msg);
        return Lists.newArrayList(filePath);
    }

    private void appendGroup(Group group, String paramName, Object value, FieldType fieldType) {
        switch (fieldType.getDataType()) {
            case Int8:
            case Int16:
            case Int32:
                group.append(paramName, Integer.parseInt(value.toString()));
                break;
            case Int64:
                group.append(paramName, Long.parseLong(value.toString()));
                break;
            case Float:
                group.append(paramName, Float.parseFloat(value.toString()));
                break;
            case Double:
                group.append(paramName, Double.parseDouble(value.toString()));
                break;
            case Bool:
                group.append(paramName, Boolean.parseBoolean(value.toString()));
                break;
            case VarChar:
            case String:
                group.append(paramName, String.valueOf(value));
                break;
            case JSON:
                group.append(paramName, ((JSONObject) value).toJSONString());
                break;
            case FloatVector:
                addFloatArray(group, paramName, (List<Float>) value);
                break;
            case BinaryVector:
                addBinaryVector(group, paramName, (ByteBuffer) value);
                break;
            case Array:
                switch (fieldType.getElementType()) {
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

    private static void addBinaryVector(Group group, String fieldName, ByteBuffer byteBuffer) {
        Group arrayGroup = group.addGroup(fieldName);
        byte[] bytes = byteBuffer.array();
        for (byte value : bytes) {
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
}
