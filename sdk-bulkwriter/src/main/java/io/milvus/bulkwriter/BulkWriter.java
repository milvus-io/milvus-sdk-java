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

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.TypeSize;
import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.bulkwriter.writer.CSVFileWriter;
import io.milvus.bulkwriter.writer.FormatFileWriter;
import io.milvus.bulkwriter.writer.JSONFileWriter;
import io.milvus.bulkwriter.writer.ParquetFileWriter;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.common.utils.Float16Utils;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.utils.SchemaUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public abstract class BulkWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);
    protected CreateCollectionReq.CollectionSchema collectionSchema;
    protected long chunkSize;

    protected BulkFileType fileType;
    protected String localPath;
    protected String uuid;
    protected int flushCount;
    protected FormatFileWriter fileWriter;
    protected final Map<String, Object> config;

    protected long totalSize;
    protected long totalRowCount;
    protected ReentrantLock appendLock;
    protected ReentrantLock fileWriteLock;

    protected boolean firstWrite;

    protected BulkWriter(CreateCollectionReq.CollectionSchema collectionSchema, long chunkSize, BulkFileType fileType, String localPath, Map<String, Object> config) throws IOException {
        this.collectionSchema = collectionSchema;
        this.chunkSize = chunkSize;
        this.fileType = fileType;
        this.localPath = localPath;
        this.uuid = UUID.randomUUID().toString();
        this.config = config;

        if (CollectionUtils.isEmpty(collectionSchema.getFieldSchemaList())) {
            ExceptionUtils.throwUnExpectedException("collection schema fields list is empty");
        }

        if (!hasPrimaryField(collectionSchema.getFieldSchemaList())) {
            ExceptionUtils.throwUnExpectedException("primary field is null");
        }
        appendLock = new ReentrantLock();

        this.makeDir();
        fileWriteLock = new ReentrantLock();
        fileWriter = null;
        this.newFileWriter();

        firstWrite = true;
    }

    protected Long getTotalSize() {
        return totalSize;
    }

    public Long getTotalRowCount() {
        return totalRowCount;
    }

    protected Long getChunkSize() {
        return chunkSize;
    }

    protected FormatFileWriter getFileWriter() {
        return fileWriter;
    }

    protected FormatFileWriter newFileWriter() throws IOException {
        FormatFileWriter oldFileWriter = fileWriter;

        fileWriteLock.lock();
        createWriterByType();
        fileWriteLock.unlock();
        return oldFileWriter;
    }

    private void createWriterByType() throws IOException {
        flushCount += 1;
        java.nio.file.Path path = Paths.get(localPath);
        java.nio.file.Path filePathPrefix = path.resolve(String.valueOf(flushCount));

        switch (fileType) {
            case PARQUET:
                this.fileWriter =  new ParquetFileWriter(collectionSchema, filePathPrefix.toString());
                break;
            case JSON:
                this.fileWriter = new JSONFileWriter(collectionSchema, filePathPrefix.toString());
                break;
            case CSV:
                this.fileWriter = new CSVFileWriter(collectionSchema, filePathPrefix.toString(), config);
                break;
            default:
                ExceptionUtils.throwUnExpectedException("Unsupported file type: " + fileType);
        }
    }

    private void makeDir() throws IOException {
        java.nio.file.Path path = Paths.get(localPath);
        createDirIfNotExist(path);

        java.nio.file.Path fullPath = path.resolve(uuid);
        createDirIfNotExist(fullPath);
        this.localPath = fullPath.toString();
    }

    private void createDirIfNotExist(java.nio.file.Path path) throws IOException {
        try {
            Files.createDirectories(path);
            logger.info("Data path created: {}", path);
        } catch (IOException e) {
            logger.error("Data Path create failed: {}", path);
            throw e;
        }
    }

    public void appendRow(JsonObject row) throws IOException, InterruptedException {
        Map<String, Object> rowValues = verifyRow(row);
        List<String> filePaths = Lists.newArrayList();

        appendLock.lock();
        fileWriter.appendRow(rowValues, firstWrite);
        firstWrite = false;
        if (getTotalSize() > getChunkSize()) {
            filePaths = commitIfFileReady(true);
        }
        appendLock.unlock();

        if (CollectionUtils.isNotEmpty(filePaths)) {
            callBackIfCommitReady(filePaths);
        }
    }


    protected abstract List<String> commitIfFileReady(boolean createNewFile) throws IOException;

    protected abstract void callBackIfCommitReady(List<String> filePaths) throws IOException, InterruptedException;


    protected void commit() {
        appendLock.lock();
        totalSize = 0;
        totalRowCount = 0;
        appendLock.unlock();
    }

    protected String getDataPath() {
        return "";
    }

    private JsonElement setDefaultValue(Object defaultValue, JsonObject row, String fieldName) {
        if (defaultValue instanceof Boolean) {
            row.addProperty(fieldName, (Boolean) defaultValue);
            return new JsonPrimitive((Boolean) defaultValue);
        } else if (defaultValue instanceof String) {
            row.addProperty(fieldName, (String) defaultValue);
            return new JsonPrimitive((String) defaultValue);
        } else {
            row.addProperty(fieldName, (Number) defaultValue);
            return new JsonPrimitive((Number) defaultValue);
        }
    }

    protected Map<String, Object> verifyRow(JsonObject row) {
        int rowSize = 0;
        Map<String, Object> rowValues = new HashMap<>();
        List<String> outputFieldNames = V2AdapterUtils.getOutputFieldNames(collectionSchema);

        for (CreateCollectionReq.FieldSchema field : collectionSchema.getFieldSchemaList()) {
            String fieldName = field.getName();
            if (field.getIsPrimaryKey() && field.getAutoID()) {
                if (row.has(fieldName)) {
                    String msg = String.format("The primary key field '%s' is auto-id, no need to provide", fieldName);
                    ExceptionUtils.throwUnExpectedException(msg);
                } else {
                    continue;
                }
            }

            JsonElement obj = row.get(fieldName);
            if (obj == null ) {
                obj = JsonNull.INSTANCE;
            }
            if (outputFieldNames.contains(fieldName)) {
                if (obj instanceof JsonNull) {
                    continue;
                } else {
                    String msg = String.format("The field '%s'  is function output, no need to provide", fieldName);
                    ExceptionUtils.throwUnExpectedException(msg);
                }
            }

            // deal with null (None) according to the Applicable rules in this page:
            // https://milvus.io/docs/nullable-and-default.md#Nullable--Default
            Object defaultValue = field.getDefaultValue();
            if (field.getIsNullable()) {
                if (defaultValue != null) {
                    // case 1: nullable is true, default_value is not null, user_input is null
                    // replace the value by default value
                    if (obj instanceof JsonNull) {
                        obj = setDefaultValue(defaultValue, row, fieldName);
                    }

                    // case 2: nullable is true, default_value is not null, user_input is not null
                    // check and set the value
                } else {
                    // case 3: nullable is true, default_value is null, user_input is null
                    // do nothing
                    if (obj instanceof JsonNull) {
                        row.add(fieldName, JsonNull.INSTANCE);
                    }

                    // case 4: nullable is true, default_value is null, user_input is not null
                    // check and set the value
                }
            } else {
                if (defaultValue != null) {
                    // case 5: nullable is false, default_value is not null, user_input is null
                    // replace the value by default value
                    if (obj instanceof JsonNull) {
                        obj = setDefaultValue(defaultValue, row, fieldName);
                    }

                    // case 6: nullable is false, default_value is not null, user_input is not null
                    // check and set the value
                } else {
                    // case 7: nullable is false, default_value is null, user_input is null
                    // raise an exception
                    if (obj instanceof JsonNull) {
                        String msg = String.format("The field '%s' is not nullable, not allow null value", fieldName);
                        ExceptionUtils.throwUnExpectedException(msg);
                    }

                    // case 8: nullable is false, default_value is null, user_input is not null
                    // check and set the value
                }
            }

            DataType dataType = field.getDataType();
            switch (dataType) {
                case BinaryVector:
                case FloatVector:
                case Float16Vector:
                case BFloat16Vector:
                case SparseFloatVector:
                case Int8Vector:{
                    Pair<Object, Integer> objectAndSize = verifyVector(obj, field);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case VarChar: {
                    Pair<Object, Integer> objectAndSize = verifyVarchar(obj, field);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case JSON: {
                    Pair<Object, Integer> objectAndSize = verifyJSON(obj, field);
                    rowValues.put(fieldName, objectAndSize.getLeft());
                    rowSize += objectAndSize.getRight();
                    break;
                }
                case Array: {
                    Pair<Object, Integer> objectAndSize = verifyArray(obj, field);
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
                    Pair<Object, Integer> objectAndSize = verifyScalar(obj, field);
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

        appendLock.lock();
        totalSize += rowSize;
        totalRowCount += 1;
        appendLock.unlock();

        return rowValues;
    }

    private Pair<Object, Integer> verifyVector(JsonElement object, CreateCollectionReq.FieldSchema field) {
        FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(field);
        Object vector = ParamUtils.checkFieldValue(ParamUtils.ConvertField(grpcField), object);
        io.milvus.v2.common.DataType dataType = field.getDataType();
        switch (dataType) {
            case FloatVector:
                return Pair.of(vector, ((List<?>) vector).size() * 4);
            case BinaryVector:
            case Int8Vector:
                return Pair.of(vector, ((ByteBuffer)vector).limit());
            case Float16Vector:
            case BFloat16Vector:
                // for JSON and CSV, float16/bfloat16 vector is parsed as float values in text
                if (this.fileType == BulkFileType.CSV || this.fileType == BulkFileType.JSON) {
                    ByteBuffer bv = (ByteBuffer)vector;
                    bv.order(ByteOrder.LITTLE_ENDIAN); // ensure LITTLE_ENDIAN
                    List<Float> v = (dataType == DataType.Float16Vector) ?
                            Float16Utils.fp16BufferToVector(bv) : Float16Utils.bf16BufferToVector(bv);
                    return Pair.of(v, v.size() * 4);
                }
                // for PARQUET, float16/bfloat16 vector is parsed as binary
                return Pair.of(vector, ((ByteBuffer)vector).limit() * 2);
            case SparseFloatVector:
                return Pair.of(vector, ((SortedMap<Long, Float>)vector).size() * 12);
            default:
                ExceptionUtils.throwUnExpectedException("Unknown vector type");
        }
        return null;
    }

    private Pair<Object, Integer> verifyVarchar(JsonElement object, CreateCollectionReq.FieldSchema field) {
        if (object.isJsonNull()) {
            return Pair.of(null, 0);
        }

        FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(field);
        Object varchar = ParamUtils.checkFieldValue(ParamUtils.ConvertField(grpcField), object);
        return Pair.of(varchar, String.valueOf(varchar).length());
    }

    private Pair<Object, Integer> verifyJSON(JsonElement object, CreateCollectionReq.FieldSchema field) {
        if (object.isJsonNull()) {
            return Pair.of(null, 0);
        }

        String str = object.toString();
        return Pair.of(str, str.length());
    }

    private Pair<Object, Integer> verifyArray(JsonElement object, CreateCollectionReq.FieldSchema field) {
        FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(field);
        Object array = ParamUtils.checkFieldValue(ParamUtils.ConvertField(grpcField), object);
        if (array == null) {
            return Pair.of(null, 0);
        }

        int rowSize = 0;
        DataType elementType = field.getElementType();
        if (TypeSize.contains(elementType)) {
            rowSize = TypeSize.getSize(elementType) * ((List<?>)array).size();
        } else if (elementType == DataType.VarChar) {
            for (String str : (List<String>) array) {
                rowSize += str.length();
            }
        } else {
            String msg = String.format("Unsupported element type for array field '%s'", field.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }

        return Pair.of(array, rowSize);
    }

    private Pair<Object, Integer> verifyScalar(JsonElement object, CreateCollectionReq.FieldSchema field) {
        if (object.isJsonNull()) {
            return Pair.of(null, 0);
        }

        if (!object.isJsonPrimitive()) {
            String msg = String.format("Unsupported value type for field '%s'", field.getName());
            ExceptionUtils.throwUnExpectedException(msg);
        }

        JsonPrimitive value = object.getAsJsonPrimitive();
        DataType dataType = field.getDataType();
        String fieldName = field.getName();
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

    private boolean hasPrimaryField(List<CreateCollectionReq.FieldSchema> fields) {
        Optional<CreateCollectionReq.FieldSchema> primaryKeyField = fields.stream().filter(CreateCollectionReq.FieldSchema::getIsPrimaryKey).findFirst();
        return primaryKeyField.isPresent();
    }
}
