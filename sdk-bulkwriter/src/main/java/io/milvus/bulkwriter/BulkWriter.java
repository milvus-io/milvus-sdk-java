/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.bulkwriter;

import com.google.gson.*;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.TypeSize;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

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

    public void appendRow(JsonObject row) throws IOException, InterruptedException {
        Map<String, Object> rowValues = verifyRow(row);

        bufferLock.lock();
        buffer.appendRow(rowValues);
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

    private Map<String, Object> verifyRow(JsonObject row) {
        int rowSize = 0;
        Map<String, Object> rowValues = new HashMap<>();
        for (FieldType fieldType : collectionSchema.getFieldTypes()) {
            String fieldName = fieldType.getName();
            if (fieldType.isPrimaryKey() && fieldType.isAutoID()) {
                if (row.has(fieldName)) {
                    String msg = String.format("The primary key field '%s' is auto-id, no need to provide", fieldName);
                    ExceptionUtils.throwUnExpectedException(msg);
                } else {
                    continue;
                }
            }

            if (!row.has(fieldName)) {
                String msg = String.format("The field '%s' is missed in the row", fieldName);
                ExceptionUtils.throwUnExpectedException(msg);
            }

            JsonElement obj = row.get(fieldName);
            if (obj == null || obj.isJsonNull()) {
                String msg = String.format("Illegal value for field '%s', value is null", fieldName);
                ExceptionUtils.throwUnExpectedException(msg);
            }

            DataType dataType = fieldType.getDataType();
            switch (dataType) {
                case BinaryVector:
                case FloatVector:
                case Float16Vector:
                case BFloat16Vector:
                case SparseFloatVector: {
                    Pair<Object, Integer> objectAndSize = verifyVector(obj, fieldType);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case VarChar: {
                    Pair<Object, Integer> objectAndSize = verifyVarchar(obj, fieldType);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case JSON: {
                    Pair<Object, Integer> objectAndSize = verifyJSON(obj, fieldType);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case Array: {
                    Pair<Object, Integer> objectAndSize = verifyArray(obj, fieldType);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case Bool:
                case Int8:
                case Int16:
                case Int32:
                case Int64:
                case Float:
                case Double:
                    Pair<Object, Integer> objectAndSize = verifyScalar(obj, fieldType);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                default:
                    String msg = String.format("Unsupported data type of field '%s', not implemented in BulkWriter.", fieldName);
                    ExceptionUtils.throwUnExpectedException(msg);
            }
        }

        // process dynamic values
        if (this.collectionSchema.isEnableDynamicField()) {
            JsonObject dynamicValues = new JsonObject();
            if (row.has(DYNAMIC_FIELD_NAME)) {
                JsonElement value = row.get(DYNAMIC_FIELD_NAME);
                if (!(value instanceof JsonObject)) {
                    String msg = String.format("Dynamic field '%s' value should be JSON dict format", DYNAMIC_FIELD_NAME);
                    ExceptionUtils.throwUnExpectedException(msg);
                }
                dynamicValues = (JsonObject) value;
            }

            for (String key : row.keySet()) {
                if (!key.equals(DYNAMIC_FIELD_NAME) && !rowValues.containsKey(key)) {
                    dynamicValues.add(key, row.get(key));
                }
            }
            String strValues = dynamicValues.toString();
            rowValues.put(DYNAMIC_FIELD_NAME, strValues);
            rowSize += strValues.length();
        }

        bufferLock.lock();
        bufferSize += rowSize;
        bufferRowCount += 1;
        totalRowCount += 1;
        bufferLock.unlock();

        return rowValues;
    }

    private Pair<Object, Integer> verifyVector(JsonElement object, FieldType fieldType) {
        Object vector = ParamUtils.checkFieldValue(fieldType, object);
        DataType dataType = fieldType.getDataType();
        switch (dataType) {
            case FloatVector:
                return Pair.of(vector, ((List<?>) vector).size() * 4);
            case BinaryVector:
                return Pair.of(vector, ((ByteBuffer)vector).limit());
            case Float16Vector:
            case BFloat16Vector:
                return Pair.of(vector, ((ByteBuffer)vector).limit() * 2);
            case SparseFloatVector:
                return Pair.of(vector, ((SortedMap<Long, Float>)vector).size() * 12);
            default:
                ExceptionUtils.throwUnExpectedException("Unknown vector type");
        }
        return null;
    }

    private Pair<Object, Integer> verifyVarchar(JsonElement object, FieldType fieldType) {
        Object varchar = ParamUtils.checkFieldValue(fieldType, object);
        return Pair.of(varchar, String.valueOf(varchar).length());
    }

    private Pair<Object, Integer> verifyJSON(JsonElement object, FieldType fieldType) {
        String str = object.toString();
        return Pair.of(str, str.length());
    }

    private Pair<Object, Integer> verifyArray(JsonElement object, FieldType fieldType) {
        Object array = ParamUtils.checkFieldValue(fieldType, object);

        int rowSize = 0;
        DataType elementType = fieldType.getElementType();
        if (TypeSize.contains(elementType)) {
            rowSize = TypeSize.getSize(elementType) * ((List<?>)array).size();
        } else if (elementType == DataType.VarChar) {
            for (String str : (List<String>) array) {
                rowSize += str.length();
            }
        } else {
            String msg = String.format("Unsupported element type for array field '%s'", fieldType.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }

        return Pair.of(array, rowSize);
    }

    private Pair<Object, Integer> verifyScalar(JsonElement object, FieldType fieldType) {
        if (!object.isJsonPrimitive()) {
            String msg = String.format("Unsupported value type for field '%s'", fieldType.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }

        JsonPrimitive value = object.getAsJsonPrimitive();
        DataType dataType = fieldType.getDataType();
        String fieldName = fieldType.getName();
        if (dataType == DataType.Bool) {
            if (!value.isBoolean()) {
                String msg = String.format("Unsupported value type for field '%s', value is not boolean", fieldName);
                ExceptionUtils.throwUnExpectedException(msg);
            }
            return Pair.of(value.getAsBoolean(), TypeSize.getSize(dataType));
        } else {
            if (!value.isNumber()) {
                String msg = String.format("Unsupported value type for field '%s', value is not a number", fieldName);
                ExceptionUtils.throwUnExpectedException(msg);
            }

            switch (dataType) {
                case Int8:
                case Int16:
                    return Pair.of(value.getAsShort(), TypeSize.getSize(dataType));
                case Int32:
                    return Pair.of(value.getAsInt(), TypeSize.getSize(dataType));
                case Int64:
                    return Pair.of(value.getAsLong(), TypeSize.getSize(dataType));
                case Float:
                    return Pair.of(value.getAsFloat(), TypeSize.getSize(dataType));
                case Double:
                    return Pair.of(value.getAsDouble(), TypeSize.getSize(dataType));
                default:
                    String msg = String.format("Field '%s' is not a scalar field", fieldName);
                    ExceptionUtils.throwUnExpectedException(msg);
            }
        }
        return Pair.of(null, null);
    }

    private boolean hasPrimaryField(List<FieldType> fieldTypes) {
        Optional<FieldType> primaryKeyField = fieldTypes.stream().filter(FieldType::isPrimaryKey).findFirst();
        return primaryKeyField.isPresent();
    }
}
