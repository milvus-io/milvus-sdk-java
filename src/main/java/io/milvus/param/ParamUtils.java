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

package io.milvus.param;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.GTsDict;
import io.milvus.common.utils.JsonUtils;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.BaseRanker;
import io.milvus.response.DescCollResponseWrapper;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions for param classes
 */
public class ParamUtils {

    private static HashMap<DataType, String> getTypeErrorMsgForColumnInsert() {
        final HashMap<DataType, String> typeErrMsg = new HashMap<>();
        typeErrMsg.put(DataType.None, "Type mismatch for field '%s': the field type is illegal.");
        typeErrMsg.put(DataType.Bool, "Type mismatch for field '%s': Bool field value type must be Boolean.");
        typeErrMsg.put(DataType.Int8, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer.");
        typeErrMsg.put(DataType.Int16, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer.");
        typeErrMsg.put(DataType.Int32, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer.");
        typeErrMsg.put(DataType.Int64, "Type mismatch for field '%s': Int64 field value type must be Long.");
        typeErrMsg.put(DataType.Float, "Type mismatch for field '%s': Float field value type must be Float.");
        typeErrMsg.put(DataType.Double, "Type mismatch for field '%s': Double field value type must be Double.");
        typeErrMsg.put(DataType.String, "Type mismatch for field '%s': String field value type must be String."); // actually String type is useless
        typeErrMsg.put(DataType.VarChar, "Type mismatch for field '%s': VarChar field value type must be String, and the string length must shorter than max_length.");
        typeErrMsg.put(DataType.Array, "Type mismatch for field '%s': Array field value type must be List<Object>, each object type must be element_type, and the array length must be shorter than max_capacity.");
        typeErrMsg.put(DataType.FloatVector, "Type mismatch for field '%s': Float vector field's value type must be List<Float>.");
        typeErrMsg.put(DataType.BinaryVector, "Type mismatch for field '%s': Binary vector field's value type must be ByteBuffer.");
        typeErrMsg.put(DataType.Float16Vector, "Type mismatch for field '%s': Float16 vector field's value type must be ByteBuffer.");
        typeErrMsg.put(DataType.BFloat16Vector, "Type mismatch for field '%s': BFloat16 vector field's value type must be ByteBuffer.");
        typeErrMsg.put(DataType.SparseFloatVector, "Type mismatch for field '%s': SparseFloatVector vector field's value type must be SortedMap<Long, Float>.");
        return typeErrMsg;
    }

    private static HashMap<DataType, String> getTypeErrorMsgForRowInsert() {
        final HashMap<DataType, String> typeErrMsg = new HashMap<>();
        typeErrMsg.put(DataType.None, "Type mismatch for field '%s': the field type is illegal.");
        typeErrMsg.put(DataType.Bool, "Type mismatch for field '%s': Bool field value type must be JsonPrimitive.");
        typeErrMsg.put(DataType.Int8, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be JsonPrimitive of boolean.");
        typeErrMsg.put(DataType.Int16, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be JsonPrimitive of number.");
        typeErrMsg.put(DataType.Int32, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be JsonPrimitive of number.");
        typeErrMsg.put(DataType.Int64, "Type mismatch for field '%s': Int64 field value type must be JsonPrimitive of number.");
        typeErrMsg.put(DataType.Float, "Type mismatch for field '%s': Float field value type must be JsonPrimitive of number.");
        typeErrMsg.put(DataType.Double, "Type mismatch for field '%s': Double field value type must be JsonPrimitive of number.");
        typeErrMsg.put(DataType.String, "Type mismatch for field '%s': String field value type must be JsonPrimitive of string."); // actually String type is useless
        typeErrMsg.put(DataType.VarChar, "Type mismatch for field '%s': VarChar field value type must be JsonPrimitive of string, and the string length must shorter than max_length.");
        typeErrMsg.put(DataType.Array, "Type mismatch for field '%s': Array field value type must be JsonArray, each object type must be element_type, and the array length must be shorter than max_capacity.");
        typeErrMsg.put(DataType.FloatVector, "Type mismatch for field '%s': Float vector field's value type must be JsonArray of List<Float>.");
        typeErrMsg.put(DataType.BinaryVector, "Type mismatch for field '%s': Binary vector field's value type must be JsonArray of byte[].");
        typeErrMsg.put(DataType.Float16Vector, "Type mismatch for field '%s': Float16 vector field's value type must be JsonArray of byte[].");
        typeErrMsg.put(DataType.BFloat16Vector, "Type mismatch for field '%s': BFloat16 vector field's value type must be JsonArray of byte[].");
        typeErrMsg.put(DataType.SparseFloatVector, "Type mismatch for field '%s': SparseFloatVector vector field's value type must be JsonObject of Map<Long, Float>.");
        return typeErrMsg;
    }

    public static void checkFieldData(FieldType fieldSchema, InsertParam.Field fieldData) {
        List<?> values = fieldData.getValues();
        checkFieldData(fieldSchema, values, false);
    }

    private static int calculateBinVectorDim(DataType dataType, int byteCount) {
        if (dataType == DataType.BinaryVector) {
            return byteCount*8; // for BinaryVector, each byte is 8 dimensions
        } else {
            if (byteCount%2 != 0) {
                String msg = "Incorrect byte count for %s type field, byte count is %d, cannot be evenly divided by 2";
                throw new ParamException(String.format(msg, dataType.name(), byteCount));
            }
            return byteCount/2; // for float16/bfloat16, each dimension is 2 bytes
        }
    }

    public static void checkFieldData(FieldType fieldSchema, List<?> values, boolean verifyElementType) {
        HashMap<DataType, String> errMsgs = getTypeErrorMsgForColumnInsert();
        DataType dataType = verifyElementType ? fieldSchema.getElementType() : fieldSchema.getDataType();

        if (verifyElementType && values.size() > fieldSchema.getMaxCapacity()) {
            throw new ParamException(String.format("Array field '%s' length: %d exceeds max capacity: %d",
                    fieldSchema.getName(), values.size(), fieldSchema.getMaxCapacity()));
        }

        switch (dataType) {
            case FloatVector: {
                int dim = fieldSchema.getDimension();
                for (int i = 0; i < values.size(); ++i) {
                    // is List<> ?
                    Object value  = values.get(i);
                    if (!(value instanceof List)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                    // is List<Float> ?
                    List<?> temp = (List<?>)value;
                    for (Object v : temp) {
                        if (!(v instanceof Float)) {
                            throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                        }
                    }

                    // check dimension
                    if (temp.size() != dim) {
                        String msg = "Incorrect dimension for field '%s': the no.%d vector's dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), i, temp.size(), dim));
                    }
                }
                break;
            }
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector: {
                int dim = fieldSchema.getDimension();
                for (int i = 0; i < values.size(); ++i) {
                    Object value  = values.get(i);
                    // is ByteBuffer?
                    if (!(value instanceof ByteBuffer)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }

                    // check dimension
                    ByteBuffer v = (ByteBuffer)value;
                    int real_dim = calculateBinVectorDim(dataType, v.position());
                    if (real_dim != dim) {
                        String msg = "Incorrect dimension for field '%s': the no.%d vector's dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), i, real_dim, dim));
                    }
                }
                break;
            }
            case SparseFloatVector:
                for (Object value : values) {
                    if (!(value instanceof SortedMap)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }

                    // is SortedMap<Long, Float> ?
                    SortedMap<?, ?> m = (SortedMap<?, ?>)value;
                    if (m.isEmpty()) { // not allow empty value for sparse vector
                        String msg = "Not allow empty SortedMap for sparse vector field '%s'";
                        throw new ParamException(String.format(msg, fieldSchema.getName()));
                    }
                    if (!(m.firstKey() instanceof Long) || !(m.get(m.firstKey()) instanceof Float)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Int64:
                for (Object value : values) {
                    if (!(value instanceof Long)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Int32:
            case Int16:
            case Int8:
                for (Object value : values) {
                    if (!(value instanceof Short) && !(value instanceof Integer)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Bool:
                for (Object value : values) {
                    if (!(value instanceof Boolean)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Float:
                for (Object value : values) {
                    if (!(value instanceof Float)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Double:
                for (Object value : values) {
                    if (!(value instanceof Double)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case VarChar:
            case String:
                for (Object value : values) {
                    if (!(value instanceof String)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                    if (((String) value).length() > fieldSchema.getMaxLength()) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case JSON:
                for (Object value : values) {
                    if (!(value instanceof JsonElement)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Array:
                for (Object value : values) {
                    if (!(value instanceof List)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }

                    List<?> array = (List<?>)value;
                    if (array.size() > fieldSchema.getMaxCapacity()) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                    checkFieldData(fieldSchema, array, true);
                }
                break;
            default:
                throw new ParamException("Unsupported data type returned by FieldData");
        }
    }

    public static Object checkFieldValue(FieldType fieldSchema, JsonElement value) {
        HashMap<DataType, String> errMsgs = getTypeErrorMsgForRowInsert();
        DataType dataType = fieldSchema.getDataType();

        switch (dataType) {
            case FloatVector: {
                if (!(value.isJsonArray())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                int dim = fieldSchema.getDimension();
                try {
                    List<Float> vector = JsonUtils.fromJson(value, new TypeToken<List<Float>>() {}.getType());
                    if (vector.size() != dim) {
                        String msg = "Incorrect dimension for field '%s': dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), vector.size(), dim));
                    }
                    return vector; // return List<Float> for genFieldData()
                } catch (JsonSyntaxException e) {
                    throw new ParamException(String.format("Unable to convert JsonArray to List<Float> for field '%s'. Reason: %s",
                            fieldSchema.getName(), e.getCause().getMessage()));
                }
            }
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector: {
                if (!(value.isJsonArray())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                int dim = fieldSchema.getDimension();
                try {
                    byte[] v = JsonUtils.fromJson(value, new TypeToken<byte[]>() {}.getType());
                    int real_dim = calculateBinVectorDim(dataType, v.length);
                    if (real_dim != dim) {
                        String msg = "Incorrect dimension for field '%s': dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), real_dim, dim));
                    }
                    return ByteBuffer.wrap(v); // return ByteBuffer for genFieldData()
                } catch (JsonSyntaxException e) {
                    throw new ParamException(String.format("Unable to convert JsonArray to List<Float> for field '%s'. Reason: %s",
                            fieldSchema.getName(), e.getCause().getMessage()));
                }
            }
            case SparseFloatVector:
                if (!(value.isJsonObject())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                try {
                    // return SortedMap<Long, Float> for genFieldData()
                    return JsonUtils.fromJson(value, new TypeToken<SortedMap<Long, Float>>() {}.getType());
                } catch (JsonSyntaxException e) {
                    throw new ParamException(String.format("Unable to convert JsonObject to SortedMap<Long, Float> for field '%s'. Reason: %s",
                            fieldSchema.getName(), e.getCause().getMessage()));
                }
            case Int64:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return value.getAsLong(); // return long for genFieldData()
            case Int32:
            case Int16:
            case Int8:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return value.getAsInt(); // return int for genFieldData()
            case Bool:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return value.getAsBoolean(); // return boolean for genFieldData()
            case Float:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return value.getAsFloat(); // return float for genFieldData()
            case Double:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return value.getAsDouble(); // return double for genFieldData()
            case VarChar:
            case String:
                if (!(value.isJsonPrimitive())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                JsonPrimitive p = value.getAsJsonPrimitive();
                if (!p.isString()) {
                    throw new ParamException(String.format("JsonPrimitive should be String type for field '%s'", fieldSchema.getName()));
                }

                String str = p.getAsString();
                if (str.length() > fieldSchema.getMaxLength()) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return str; // return String for genFieldData()
            case JSON:
                return value; // return JsonElement for genFieldData()
            case Array:
                if (!(value.isJsonArray())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }

                List<Object> array = convertJsonArray(value.getAsJsonArray(), fieldSchema.getElementType(), fieldSchema.getName());
                if (array.size() > fieldSchema.getMaxCapacity()) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                }
                return array; // return List<Object> for genFieldData()
            default:
                throw new ParamException("Unsupported data type returned by FieldData");
        }
    }

    public static List<Object> convertJsonArray(JsonArray jsonArray, DataType elementType, String fieldName) {
        try {
            switch (elementType) {
                case Int64:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<Long>>() {}.getType());
                case Int32:
                case Int16:
                case Int8:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<Integer>>() {}.getType());
                case Bool:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<Boolean>>() {}.getType());
                case Float:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<Float>>() {}.getType());
                case Double:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<Double>>() {}.getType());
                case VarChar:
                    return JsonUtils.fromJson(jsonArray, new TypeToken<List<String>>() {}.getType());
                default:
                    throw new ParamException(String.format("Unsupported element type of Array field '%s'", fieldName));
            }
        } catch (JsonSyntaxException e) {
            throw new ParamException(String.format("Unable to convert JsonArray to List<Object> for field '%s'. Reason: %s",
                    fieldName, e.getCause().getMessage()));
        }
    }

    /**
     * Checks if a string is empty or null.
     * Throws {@link ParamException} if the string is empty of null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullEmptyString(String target, String name) throws ParamException {
        if (target == null || StringUtils.isBlank(target)) {
            throw new ParamException(name + " cannot be null or empty");
        }
    }

    /**
     * Checks if a string is  null.
     * Throws {@link ParamException} if the string is null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullString(String target, String name) throws ParamException {
        if (target == null) {
            throw new ParamException(name + " cannot be null");
        }
    }

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;

        public InsertBuilderWrapper(@NonNull InsertParam requestParam,
                                    DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getRowCount());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                insertBuilder.setDbName(requestParam.getDatabaseName());
            }
            fillFieldsData(requestParam, wrapper);
        }

        public InsertBuilderWrapper(@NonNull UpsertParam requestParam,
                                    DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // currently, not allow to upsert for collection whose primary key is auto-generated
            FieldType pk = wrapper.getPrimaryField();
            if (pk.isAutoID()) {
                throw new ParamException(String.format("Upsert don't support autoID==True, collection: %s",
                        requestParam.getCollectionName()));
            }

            // generate upsert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getRowCount());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                upsertBuilder.setDbName(requestParam.getDatabaseName());
            }
            fillFieldsData(requestParam, wrapper);
        }

        private void addFieldsData(io.milvus.grpc.FieldData value) {
            if (insertBuilder != null) {
                insertBuilder.addFieldsData(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.addFieldsData(value);
            }
        }

        private void setPartitionName(String value) {
            if (insertBuilder != null) {
                insertBuilder.setPartitionName(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.setPartitionName(value);
            }
        }

        private void fillFieldsData(InsertParam requestParam, DescCollResponseWrapper wrapper) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            boolean isPartitionKeyEnabled = false;
            for (FieldType fieldType : wrapper.getFields()) {
                if (fieldType.isPartitionKey()) {
                    isPartitionKeyEnabled = true;
                    break;
                }
            }
            if (isPartitionKeyEnabled) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = String.format("Collection %s has partition key, not allow to specify partition name",
                            requestParam.getCollectionName());
                    throw new ParamException(msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<InsertParam.Field> columnFields = requestParam.getFields();
            List<JsonObject> rowFields = requestParam.getRows();

            if (CollectionUtils.isNotEmpty(columnFields)) {
                checkAndSetColumnData(wrapper, columnFields);
            } else {
                checkAndSetRowData(wrapper, rowFields);
            }
        }

        private void checkAndSetColumnData(DescCollResponseWrapper wrapper, List<InsertParam.Field> fields) {
            List<FieldType> fieldTypes = wrapper.getFields();

            // gen fieldData
            // make sure the field order must be consisted with collection schema
            for (FieldType fieldType : fieldTypes) {
                boolean found = false;
                for (InsertParam.Field field : fields) {
                    if (field.getName().equals(fieldType.getName())) {
                        if (fieldType.isAutoID()) {
                            String msg = String.format("The primary key: %s is auto generated, no need to input.",
                                    fieldType.getName());
                            throw new ParamException(msg);
                        }
                        checkFieldData(fieldType, field);

                        found = true;
                        this.addFieldsData(genFieldData(fieldType, field.getValues()));
                        break;
                    }

                }
                if (!found && !fieldType.isAutoID()) {
                    throw new ParamException(String.format("The field: %s is not provided.", fieldType.getName()));
                }
            }

            // deal with dynamicField
            if (wrapper.getEnableDynamicField()) {
                for (InsertParam.Field field : fields) {
                    if (field.getName().equals(Constant.DYNAMIC_FIELD_NAME)) {
                        FieldType dynamicType = FieldType.newBuilder()
                                .withName(Constant.DYNAMIC_FIELD_NAME)
                                .withDataType(DataType.JSON)
                                .withIsDynamic(true)
                                .build();
                        checkFieldData(dynamicType, field);
                        this.addFieldsData(genFieldData(dynamicType, field.getValues(), true));
                        break;
                    }
                }
            }
        }

        private void checkAndSetRowData(DescCollResponseWrapper wrapper, List<JsonObject> rows) {
            List<FieldType> fieldTypes = wrapper.getFields();

            Map<String, InsertDataInfo> nameInsertInfo = new HashMap<>();
            InsertDataInfo insertDynamicDataInfo = InsertDataInfo.builder().fieldType(
                    FieldType.newBuilder()
                            .withName(Constant.DYNAMIC_FIELD_NAME)
                            .withDataType(DataType.JSON)
                            .withIsDynamic(true)
                            .build())
                    .data(new LinkedList<>()).build();
            for (JsonObject row : rows) {
                for (FieldType fieldType : fieldTypes) {
                    String fieldName = fieldType.getName();
                    InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, InsertDataInfo.builder()
                            .fieldType(fieldType).data(new LinkedList<>()).build());

                    // check normalField
                    JsonElement rowFieldData = row.get(fieldName);
                    if (rowFieldData != null && !rowFieldData.isJsonNull()) {
                        if (fieldType.isAutoID()) {
                            String msg = String.format("The primary key: %s is auto generated, no need to input.", fieldName);
                            throw new ParamException(msg);
                        }
                        Object fieldValue = checkFieldValue(fieldType, rowFieldData);
                        insertDataInfo.getData().add(fieldValue);
                        nameInsertInfo.put(fieldName, insertDataInfo);
                    } else {
                        // check if autoId
                        if (!fieldType.isAutoID()) {
                            String msg = String.format("The field: %s is not provided.", fieldType.getName());
                            throw new ParamException(msg);
                        }
                    }
                }

                // deal with dynamicField
                if (wrapper.getEnableDynamicField()) {
                    JsonObject dynamicField = new JsonObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!nameInsertInfo.containsKey(rowFieldName)) {
                            dynamicField.add(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.getData().add(dynamicField);
                }
            }

            for (String fieldNameKey : nameInsertInfo.keySet()) {
                InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
                this.addFieldsData(genFieldData(insertDataInfo.getFieldType(), insertDataInfo.getData()));
            }
            if (wrapper.getEnableDynamicField()) {
                this.addFieldsData(genFieldData(insertDynamicDataInfo.getFieldType(), insertDynamicDataInfo.getData(), Boolean.TRUE));
            }
        }

        public InsertRequest buildInsertRequest() {
            if (insertBuilder != null) {
                return insertBuilder.build();
            }
            throw new ParamException("Unable to build insert request since no input");
        }

        public UpsertRequest buildUpsertRequest() {
            if (upsertBuilder != null) {
                return upsertBuilder.build();
            }
            throw new ParamException("Unable to build upsert request since no input");
        }
    }

    @SuppressWarnings("unchecked")
    public static ByteString convertPlaceholder(List<?> vectors, PlaceholderType placeType) throws ParamException {
        PlaceholderType plType = PlaceholderType.None;
        List<ByteString> byteStrings = new ArrayList<>();
        for (Object vector : vectors) {
            if (vector instanceof List) {
                plType = PlaceholderType.FloatVector;
                List<Float> list = (List<Float>) vector;
                ByteBuffer buf = ByteBuffer.allocate(Float.BYTES * list.size());
                buf.order(ByteOrder.LITTLE_ENDIAN); // most of operating systems default little endian
                list.forEach(buf::putFloat);

                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else if (vector instanceof ByteBuffer) {
                // for fp16/bf16 vector, each vector is a ByteBuffer with little endian
                // for binary vector, each vector is a ByteBuffer no matter which endian
                // the endian of each ByteBuffer is already specified by the caller
                plType = PlaceholderType.BinaryVector;
                ByteBuffer buf = (ByteBuffer) vector;
                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else if (vector instanceof SortedMap) {
                plType = PlaceholderType.SparseFloatVector;
                SortedMap<Long, Float> sparse = (SortedMap<Long, Float>) vector;
                ByteBuffer buf = encodeSparseFloatVector(sparse);
                ByteString byteString = ByteString.copyFrom(buf.array());
                byteStrings.add(byteString);
            } else {
                String msg = "Search target vector type is illegal." +
                        " Only allow List<Float> for FloatVector," +
                        " ByteBuffer for BinaryVector/Float16Vector/BFloat16Vector," +
                        " List<SortedMap<Long, Float>> for SparseFloatVector.";
                throw new ParamException(msg);
            }
        }

        // force specify PlaceholderType
        if (placeType != PlaceholderType.None) {
            plType = placeType;
        }

        PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                .setTag(Constant.VECTOR_TAG)
                .setType(plType);
        byteStrings.forEach(pldBuilder::addValues);

        PlaceholderValue plv = pldBuilder.build();
        PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                .addPlaceholders(plv)
                .build();

        return placeholderGroup.toByteString();
    }

    @SuppressWarnings("unchecked")
    public static SearchRequest convertSearchParam(@NonNull SearchParam requestParam) throws ParamException {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName());

        if (!requestParam.getPartitionNames().isEmpty()) {
            requestParam.getPartitionNames().forEach(builder::addPartitionNames);
        }
        if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
            builder.setDbName(requestParam.getDatabaseName());
        }

        // prepare target vectors
        ByteString byteStr = convertPlaceholder(requestParam.getVectors(), requestParam.getPlType());
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(requestParam.getNQ());

        // search parameters
        builder.addSearchParams(
                KeyValuePair.newBuilder()
                        .setKey(Constant.VECTOR_FIELD)
                        .setValue(requestParam.getVectorFieldName())
                        .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(requestParam.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.ROUND_DECIMAL)
                                .setValue(String.valueOf(requestParam.getRoundDecimal()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.IGNORE_GROWING)
                                .setValue(String.valueOf(requestParam.isIgnoreGrowing()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.ITERATOR_FIELD)
                                .setValue(String.valueOf(requestParam.isIterator()))
                                .build());

        if (!Objects.equals(requestParam.getMetricType(), MetricType.None.name())) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.METRIC_TYPE)
                            .setValue(requestParam.getMetricType())
                            .build());
        }

        if (!StringUtils.isEmpty(requestParam.getGroupByFieldName())) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.GROUP_BY_FIELD)
                            .setValue(requestParam.getGroupByFieldName())
                            .build());
        }

        if (null != requestParam.getParams() && !requestParam.getParams().isEmpty()) {
            try {
                Map<String, Object> paramMap = JsonUtils.fromJson(requestParam.getParams(),
                        new TypeToken<Map<String, Object>>() {}.getType());
                String offset = paramMap.getOrDefault(Constant.OFFSET, 0).toString();
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.OFFSET)
                                .setValue(offset)
                                .build());
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.PARAMS)
                                .setValue(requestParam.getParams())
                                .build());
            } catch (IllegalArgumentException e) {
                throw new ParamException(e.getMessage() + e.getCause().getMessage());
            }
        }

        if (!requestParam.getOutFields().isEmpty()) {
            requestParam.getOutFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (requestParam.getExpr() != null && !requestParam.getExpr().isEmpty()) {
            builder.setDsl(requestParam.getExpr());
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(requestParam.getConsistencyLevel(), requestParam.getCollectionName());
        builder.setTravelTimestamp(requestParam.getTravelTimestamp()); // deprecated
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (requestParam.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    public static SearchRequest convertAnnSearchParam(@NonNull AnnSearchParam annSearchParam,
                                                      ConsistencyLevelEnum consistencyLevel) {
        SearchRequest.Builder builder = SearchRequest.newBuilder();
        ByteString byteStr = convertPlaceholder(annSearchParam.getVectors(), annSearchParam.getPlType());
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(annSearchParam.getNQ());

        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.VECTOR_FIELD)
                                .setValue(annSearchParam.getVectorFieldName())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(annSearchParam.getTopK()))
                                .build());

        if (!Objects.equals(annSearchParam.getMetricType(), MetricType.None.name())) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.METRIC_TYPE)
                            .setValue(annSearchParam.getMetricType())
                            .build());
        }

        // params
        String params = "{}";
        if (null != annSearchParam.getParams() && !annSearchParam.getParams().isEmpty()) {
            params = annSearchParam.getParams();
        }
        builder.addSearchParams(KeyValuePair.newBuilder()
                .setKey(Constant.PARAMS)
                .setValue(params)
                .build());

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (annSearchParam.getExpr() != null && !annSearchParam.getExpr().isEmpty()) {
            builder.setDsl(annSearchParam.getExpr());
        }

        if (consistencyLevel == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(consistencyLevel.getCode());
        }

        return builder.build();
    }

    public static HybridSearchRequest convertHybridSearchParam(@NonNull HybridSearchParam requestParam) throws ParamException {
        HybridSearchRequest.Builder builder = HybridSearchRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName());

        if (!requestParam.getPartitionNames().isEmpty()) {
            requestParam.getPartitionNames().forEach(builder::addPartitionNames);
        }
        if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
            builder.setDbName(requestParam.getDatabaseName());
        }

        for (AnnSearchParam req : requestParam.getSearchRequests()) {
            SearchRequest searchRequest = ParamUtils.convertAnnSearchParam(req, requestParam.getConsistencyLevel());
            builder.addRequests(searchRequest);
        }

        // set ranker
        BaseRanker ranker = requestParam.getRanker();
        Map<String, String> props = ranker.getProperties();
        props.put("limit", String.format("%d", requestParam.getTopK()));
        props.put("round_decimal", String.format("%d", requestParam.getRoundDecimal()));
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(props);
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addRankParams);
        }

        // output fields
        if (!requestParam.getOutFields().isEmpty()) {
            requestParam.getOutFields().forEach(builder::addOutputFields);
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(requestParam.getConsistencyLevel(), requestParam.getCollectionName());
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        if (requestParam.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    public static QueryRequest convertQueryParam(@NonNull QueryParam requestParam) {
        long guaranteeTimestamp = getGuaranteeTimestamp(requestParam.getConsistencyLevel(), requestParam.getCollectionName());
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .addAllPartitionNames(requestParam.getPartitionNames())
                .addAllOutputFields(requestParam.getOutFields())
                .setExpr(requestParam.getExpr())
                .setTravelTimestamp(requestParam.getTravelTimestamp())
                .setGuaranteeTimestamp(guaranteeTimestamp);

        if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
            builder.setDbName(requestParam.getDatabaseName());
        }

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (requestParam.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode());
        }

        // set offset and limit value.
        // directly pass the two values, the server will verify them.
        long offset = requestParam.getOffset();
        if (offset > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.OFFSET)
                    .setValue(String.valueOf(offset))
                    .build());
        }

        long limit = requestParam.getLimit();
        if (limit > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.LIMIT)
                    .setValue(String.valueOf(limit))
                    .build());
        }

        // ignore growing
        builder.addQueryParams(KeyValuePair.newBuilder()
                .setKey(Constant.IGNORE_GROWING)
                .setValue(String.valueOf(requestParam.isIgnoreGrowing()))
                .build());

        // reduce stop for best
        builder.addQueryParams(KeyValuePair.newBuilder()
                        .setKey(Constant.REDUCE_STOP_FOR_BEST)
                        .setValue(String.valueOf(requestParam.isReduceStopForBest()))
                .build());

        // iterator
        builder.addQueryParams(KeyValuePair.newBuilder()
                        .setKey(Constant.ITERATOR_FIELD)
                        .setValue(String.valueOf(requestParam.isIterator()))
                .build());

        return builder.build();
    }

    private static long getGuaranteeTimestamp(ConsistencyLevelEnum consistencyLevel, String collectionName){
        if(consistencyLevel == null){
            Long ts = GTsDict.getInstance().getCollectionTs(collectionName);
            return  (ts == null) ? 1L : ts;
        }
        switch (consistencyLevel){
            case STRONG:
                return 0L;
            case SESSION:
                Long ts = GTsDict.getInstance().getCollectionTs(collectionName);
                return (ts == null) ? 1L : ts;
            case BOUNDED:
                return 2L; // let server side to determine the bounded time
            default:
                return 1L; // EVENTUALLY and others
        }
    }

    public static boolean isVectorDataType(DataType dataType) {
        Set<DataType> vectorDataType = new HashSet<DataType>() {{
            add(DataType.FloatVector);
            add(DataType.BinaryVector);
            add(DataType.Float16Vector);
            add(DataType.BFloat16Vector);
            add(DataType.SparseFloatVector);
        }};
        return vectorDataType.contains(dataType);
    }

    public static FieldData genFieldData(FieldType fieldType, List<?> objects) {
        return genFieldData(fieldType, objects, Boolean.FALSE);
    }

    public static FieldData genFieldData(FieldType fieldType, List<?> objects, boolean isDynamic) {
        if (objects == null) {
            throw new ParamException("Cannot generate FieldData from null object");
        }
        DataType dataType = fieldType.getDataType();
        String fieldName = fieldType.getName();
        FieldData.Builder builder = FieldData.newBuilder();
        if (isVectorDataType(dataType)) {
            VectorField vectorField = genVectorField(dataType, objects);
            return builder.setFieldName(fieldName).setType(dataType).setVectors(vectorField).build();
        } else {
            ScalarField scalarField = genScalarField(fieldType, objects);
            if (isDynamic) {
                return builder.setType(dataType).setScalars(scalarField).setIsDynamic(true).build();
            }
            return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static VectorField genVectorField(DataType dataType, List<?> objects) {
        if (dataType == DataType.FloatVector) {
            List<Float> floats = new ArrayList<>();
            // each object is List<Float>
            for (Object object : objects) {
                if (object instanceof List) {
                    List<Float> list = (List<Float>) object;
                    floats.addAll(list);
                } else {
                    throw new ParamException("The type of FloatVector must be List<Float>");
                }
            }

            int dim = floats.size() / objects.size();
            FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
            return VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
        } else if (dataType == DataType.BinaryVector ||
                dataType == DataType.Float16Vector ||
                dataType == DataType.BFloat16Vector) {
            ByteBuffer totalBuf = null;
            int dim = 0;
            // for fp16/bf16 vector, each vector is a ByteBuffer with little endian
            // for binary vector, each vector is a ByteBuffer no matter which endian
            // no need to set totalBuf endian since it is treated as byte array
            for (Object object : objects) {
                ByteBuffer buf = (ByteBuffer) object;
                if (totalBuf == null) {
                    totalBuf = ByteBuffer.allocate(buf.limit() * objects.size());
                    totalBuf.put(buf.array());
                    dim = calculateBinVectorDim(dataType, buf.limit());
                } else {
                    totalBuf.put(buf.array());
                }
            }

            assert totalBuf != null;
            ByteString byteString = ByteString.copyFrom(totalBuf.array());
            if (dataType == DataType.BinaryVector) {
                return VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
            } else if (dataType == DataType.Float16Vector) {
                return VectorField.newBuilder().setDim(dim).setFloat16Vector(byteString).build();
            } else {
                return VectorField.newBuilder().setDim(dim).setBfloat16Vector(byteString).build();
            }
        } else if  (dataType == DataType.SparseFloatVector) {
            SparseFloatArray sparseArray = genSparseFloatArray(objects);
            return VectorField.newBuilder().setDim(sparseArray.getDim()).setSparseFloatVector(sparseArray).build();
        }

        throw new ParamException("Illegal vector dataType:" + dataType);
    }

    public static ByteBuffer encodeSparseFloatVector(SortedMap<Long, Float> sparse) {
        // milvus server requires sparse vector to be transfered in little endian
        ByteBuffer buf = ByteBuffer.allocate((Integer.BYTES + Float.BYTES) * sparse.size());
        buf.order(ByteOrder.LITTLE_ENDIAN);
        for (Map.Entry<Long, Float> entry : sparse.entrySet()) {
            long k = entry.getKey();
            if (k < 0 || k >= (long)Math.pow(2.0, 32.0)-1) {
                throw new ParamException("Sparse vector index must be positive and less than 2^32-1");
            }
            // here we construct a binary from the long key
            // milvus server requires sparse vector to be transfered in little endian
            ByteBuffer lBuf = ByteBuffer.allocate(Long.BYTES);
            lBuf.order(ByteOrder.LITTLE_ENDIAN);
            lBuf.putLong(k);
            // the server requires a binary of unsigned int, append the first 4 bytes
            buf.put(lBuf.array(), 0, 4);

            float v = entry.getValue();
            if (Float.isNaN(v) || Float.isInfinite(v)) {
                throw new ParamException("Sparse vector value cannot be NaN or Infinite");
            }
            buf.putFloat(entry.getValue());
        }

        return buf;
    }

    public static SortedMap<Long, Float> decodeSparseFloatVector(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        SortedMap<Long, Float> sparse = new TreeMap<>();
        long num = buf.limit()/8; // each uint+float pair is 8 bytes
        for (long j = 0; j < num; j++) {
            // here we convert an uint 4-bytes to a long value
            // milvus server requires sparse vector to be transfered in little endian
            ByteBuffer pBuf = ByteBuffer.allocate(Long.BYTES);
            pBuf.order(ByteOrder.LITTLE_ENDIAN);
            int offset = 8*(int)j;
            byte[] aa = buf.array();
            for (int k = offset; k < offset + 4; k++) {
                pBuf.put(aa[k]); // fill the first 4 bytes with the unit bytes
            }
            pBuf.putInt(0); // fill the last 4 bytes to zero
            pBuf.rewind(); // reset position to head
            long k = pBuf.getLong(); // this is the long value converted from the uint

            // here we get the float value as normal
            buf.position(offset+4); // position offsets 4 bytes since they were converted to long
            float v = buf.getFloat(); // this is the float value
            sparse.put(k, v);
        }
        return sparse;
    }

    private static SparseFloatArray genSparseFloatArray(List<?> objects) {
        int dim = 0; // the real dim is unknown, set the max size as dim
        SparseFloatArray.Builder builder = SparseFloatArray.newBuilder();
        // each object must be SortedMap<Long, Float>, which is already validated by checkFieldData()
        for (Object object : objects) {
            if (!(object instanceof SortedMap)) {
                throw new ParamException("SparseFloatVector vector field's value type must be SortedMap");
            }
            SortedMap<Long, Float> sparse = (SortedMap<Long, Float>) object;
            dim = Math.max(dim, sparse.size());
            ByteBuffer buf = encodeSparseFloatVector(sparse);
            ByteString byteString = ByteString.copyFrom(buf.array());
            builder.addContents(byteString);
        }

        return builder.setDim(dim).build();
    }

    private static ScalarField genScalarField(FieldType fieldType, List<?> objects) {
        if (fieldType.getDataType() == DataType.Array) {
            ArrayArray.Builder builder = ArrayArray.newBuilder();
            for (Object object : objects) {
                List<?> temp = (List<?>)object;
                ScalarField arrayField = genScalarField(fieldType.getElementType(), temp);
                builder.addData(arrayField);
            }

            return ScalarField.newBuilder().setArrayData(builder.build()).build();
        } else {
            return genScalarField(fieldType.getDataType(), objects);
        }
    }

    private static ScalarField genScalarField(DataType dataType, List<?> objects) {
        switch (dataType) {
            case None:
            case UNRECOGNIZED:
                throw new ParamException("Cannot support this dataType:" + dataType);
            case Int64: {
                List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                return ScalarField.newBuilder().setLongData(longArray).build();
            }
            case Int32:
            case Int16:
            case Int8: {
                List<Integer> integers = objects.stream().map(p -> p instanceof Short ? ((Short) p).intValue() : (Integer) p).collect(Collectors.toList());
                IntArray intArray = IntArray.newBuilder().addAllData(integers).build();
                return ScalarField.newBuilder().setIntData(intArray).build();
            }
            case Bool: {
                List<Boolean> booleans = objects.stream().map(p -> (Boolean) p).collect(Collectors.toList());
                BoolArray boolArray = BoolArray.newBuilder().addAllData(booleans).build();
                return ScalarField.newBuilder().setBoolData(boolArray).build();
            }
            case Float: {
                List<Float> floats = objects.stream().map(p -> (Float) p).collect(Collectors.toList());
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                return ScalarField.newBuilder().setFloatData(floatArray).build();
            }
            case Double: {
                List<Double> doubles = objects.stream().map(p -> (Double) p).collect(Collectors.toList());
                DoubleArray doubleArray = DoubleArray.newBuilder().addAllData(doubles).build();
                return ScalarField.newBuilder().setDoubleData(doubleArray).build();
            }
            case String:
            case VarChar: {
                List<String> strings = objects.stream().map(p -> (String) p).collect(Collectors.toList());
                StringArray stringArray = StringArray.newBuilder().addAllData(strings).build();
                return ScalarField.newBuilder().setStringData(stringArray).build();
            }
            case JSON: {
                List<ByteString> byteStrings = objects.stream().map(p -> ByteString.copyFromUtf8(p.toString()))
                        .collect(Collectors.toList());
                JSONArray jsonArray = JSONArray.newBuilder().addAllData(byteStrings).build();
                return ScalarField.newBuilder().setJsonData(jsonArray).build();
            }
            default:
                throw new ParamException("Illegal scalar dataType:" + dataType);
        }
    }

    /**
     * Convert a grpc field schema to client field schema
     *
     * @param field FieldSchema object
     * @return {@link FieldType} schema of the field
     */
    public static FieldType ConvertField(@NonNull FieldSchema field) {
        FieldType.Builder builder = FieldType.newBuilder()
                .withName(field.getName())
                .withDescription(field.getDescription())
                .withPrimaryKey(field.getIsPrimaryKey())
                .withPartitionKey(field.getIsPartitionKey())
                .withClusteringKey(field.getIsClusteringKey())
                .withAutoID(field.getAutoID())
                .withDataType(field.getDataType())
                .withElementType(field.getElementType())
                .withIsDynamic(field.getIsDynamic());

        if (field.getIsDynamic()) {
            builder.withIsDynamic(true);
        }

        List<KeyValuePair> keyValuePairs = field.getTypeParamsList();
        keyValuePairs.forEach((kv) -> builder.addTypeParam(kv.getKey(), kv.getValue()));

        return builder.build();
    }

    /**
     * Convert a client field schema to grpc field schema
     *
     * @param field {@link FieldType} object
     * @return {@link FieldSchema} schema of the field
     */
    public static FieldSchema ConvertField(@NonNull FieldType field) {
        FieldSchema.Builder builder = FieldSchema.newBuilder()
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setIsPrimaryKey(field.isPrimaryKey())
                .setIsPartitionKey(field.isPartitionKey())
                .setIsClusteringKey(field.isClusteringKey())
                .setAutoID(field.isAutoID())
                .setDataType(field.getDataType())
                .setElementType(field.getElementType())
                .setIsDynamic(field.isDynamic());

        // assemble typeParams for CollectionSchema
        List<KeyValuePair> typeParamsList = AssembleKvPair(field.getTypeParams());
        if (CollectionUtils.isNotEmpty(typeParamsList)) {
            typeParamsList.forEach(builder::addTypeParams);
        }

        return builder.build();
    }

    public static List<KeyValuePair> AssembleKvPair(Map<String, String> sourceMap) {
        List<KeyValuePair> result = new ArrayList<>();

        if (MapUtils.isNotEmpty(sourceMap)) {
            sourceMap.forEach((key, value) -> {
                KeyValuePair kv = KeyValuePair.newBuilder()
                        .setKey(key)
                        .setValue(value).build();
                result.add(kv);
            });
        }
        return result;
    }

    @Builder
    @Getter
    public static class InsertDataInfo {
        private final FieldType fieldType;
        private final LinkedList<Object> data;
    }
}
