package io.milvus.bulkwriter;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.TypeSize;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);
    protected CollectionSchemaParam collectionSchema;
    protected int chunkSize;
    protected BulkFileType fileType;

    protected int bufferSize;
    protected int bufferRowCount;
    protected int totalRowCount;
    protected Buffer buffer;
    protected ReentrantLock bufferLock;

    protected BulkWriter(CollectionSchemaParam collectionSchema, int chunkSize, BulkFileType fileType) {
        this.collectionSchema = collectionSchema;
        this.chunkSize = chunkSize;
        this.fileType = fileType;

        if (CollectionUtils.isEmpty(collectionSchema.getFieldTypes())) {
            ExceptionUtils.throwUnExpectedException("collection schema fields list is empty");
        }

        if (!hasPrimaryField(collectionSchema.getFieldTypes())) {
            ExceptionUtils.throwUnExpectedException("primary field is null");
        }
        bufferLock = new ReentrantLock();
        buffer = null;
        this.newBuffer();
    }

    protected Integer getBufferSize() {
        return bufferSize;
    }

    public Integer getBufferRowCount() {
        return bufferRowCount;
    }

    public Integer getTotalRowCount() {
        return totalRowCount;
    }

    protected Integer getChunkSize() {
        return chunkSize;
    }

    protected Buffer newBuffer() {
        Buffer oldBuffer = buffer;

        bufferLock.lock();
        this.buffer = new Buffer(collectionSchema, fileType);
        bufferLock.unlock();

        return oldBuffer;
    }

    public void appendRow(JSONObject row) throws IOException, InterruptedException {
        verifyRow(row);

        bufferLock.lock();
        buffer.appendRow(row);
        bufferLock.unlock();
    }

    protected void commit(boolean async) throws InterruptedException {
        bufferLock.lock();
        bufferSize = 0;
        bufferRowCount = 0;
        bufferLock.unlock();
    }

    protected String getDataPath() {
        return "";
    }

    private void verifyRow(JSONObject row) {
        int rowSize = 0;
        for (FieldType fieldType : collectionSchema.getFieldTypes()) {
            if (fieldType.isPrimaryKey() && fieldType.isAutoID()) {
                if (row.containsKey(fieldType.getName())) {
                    String msg = String.format("The primary key field '%s' is auto-id, no need to provide", fieldType.getName());
                    ExceptionUtils.throwUnExpectedException(msg);
                } else {
                    continue;
                }
            }

            if (!row.containsKey(fieldType.getName())) {
                String msg = String.format("The field '%s' is missed in the row", fieldType.getName());
                ExceptionUtils.throwUnExpectedException(msg);
            }

            switch (fieldType.getDataType()) {
                case BinaryVector:
                case FloatVector:
                    rowSize += verifyVector(row.get(fieldType.getName()), fieldType);
                    break;
                case VarChar:
                    rowSize += verifyVarchar(row.get(fieldType.getName()), fieldType, false);
                    break;
                case JSON:
                    Pair<Object, Integer> objectRowSize = verifyJSON(row.get(fieldType.getName()), fieldType);
                    row.put(fieldType.getName(), objectRowSize.getLeft());
                    rowSize += objectRowSize.getRight();
                    break;
                case Array:
                    rowSize += verifyArray(row.get(fieldType.getName()), fieldType);
                    break;
                default:
                    rowSize += TypeSize.getSize(fieldType.getDataType());
            }
        }

        bufferLock.lock();
        bufferSize += rowSize;
        bufferRowCount += 1;
        totalRowCount += 1;
        bufferLock.unlock();
    }

    private Integer verifyVector(Object object, FieldType fieldType) {
        if (fieldType.getDataType() == DataType.FloatVector) {
            ParamUtils.checkFieldData(fieldType, Lists.newArrayList(object), false);
            return ((List<?>)object).size() * 4;
        } else {
            ParamUtils.checkFieldData(fieldType, Lists.newArrayList(object), false);
            return ((ByteBuffer)object).position();
        }
    }

    private Integer verifyVarchar(Object object, FieldType fieldType, boolean verifyElementType) {
        ParamUtils.checkFieldData(fieldType, Lists.newArrayList(object), verifyElementType);

        return String.valueOf(object).length();
    }

    private Pair<Object, Integer> verifyJSON(Object object, FieldType fieldType) {
        int size = 0;
        if (object instanceof String) {
            size = String.valueOf(object).length();
            object = tryConvertJson(fieldType.getName(), object);
        } else if (object instanceof JSONObject) {
            size = ((JSONObject) object).toJSONString().length();
        } else {
            String msg = String.format("Illegal JSON value for field '%s', type mismatch", fieldType.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }
        return Pair.of(object, size);
    }

    private Integer verifyArray(Object object, FieldType fieldType) {
        ParamUtils.checkFieldData(fieldType, (List<?>)object, true);

        int rowSize = 0;
        DataType elementType = fieldType.getElementType();
        if (TypeSize.contains(elementType)) {
            rowSize = TypeSize.getSize(elementType) * ((List<?>)object).size();
        } else if (elementType == DataType.VarChar) {
            for (String ele : (List<String>) object) {
                rowSize += verifyVarchar(ele, fieldType, true);
            }
        } else {
            String msg = String.format("Unsupported element type for array field '%s'", fieldType.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }

        return rowSize;
    }

    private Object tryConvertJson(String fieldName, Object object) {
        if (object instanceof String) {
            try {
                return JSONObject.parseObject(String.valueOf(object));
            } catch (Exception e) {
                String msg = String.format("Illegal JSON value for field '%s', type mismatch or illegal format, error: %s", fieldName, e);
                ExceptionUtils.throwUnExpectedException(msg);
            }
        }
        return object;
    }

    private boolean hasPrimaryField(List<FieldType> fieldTypes) {
        Optional<FieldType> primaryKeyField = fieldTypes.stream().filter(FieldType::isPrimaryKey).findFirst();
        return primaryKeyField.isPresent();
    }
}
