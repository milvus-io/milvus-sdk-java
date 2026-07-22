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

package io.milvus.v2.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.milvus.grpc.*;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.exception.DataNotMatchException;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class DataUtils {

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;

        public InsertRequest convertGrpcInsertRequest(InsertReq requestParam, DescribeCollectionResp descColl) {
            String dbName = requestParam.getDatabaseName();
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getData().size());
            if (StringUtils.isNotEmpty(dbName)) {
                insertBuilder.setDbName(dbName);
            }
            upsertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return insertBuilder.build();
        }

        public UpsertRequest convertGrpcUpsertRequest(UpsertReq requestParam, DescribeCollectionResp descColl) {
            String dbName = requestParam.getDatabaseName();
            String collectionName = requestParam.getCollectionName();

            // generate upsert request builder
            List<FieldPartialUpdateOp> fieldOps = convertFieldOps(requestParam.getFieldOps());
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Upsert).build();

            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setPartialUpdate(requestParam.isPartialUpdate())
                    .addAllFieldOps(fieldOps)
                    .setNumRows(requestParam.getData().size());
            if (StringUtils.isNotEmpty(dbName)) {
                upsertBuilder.setDbName(dbName);
            }
            insertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return upsertBuilder.build();
        }

        private static List<FieldPartialUpdateOp> convertFieldOps(List<UpsertReq.FieldPartialUpdateOp> requestFieldOps) {
            if (requestFieldOps == null) {
                return Collections.emptyList();
            }

            List<FieldPartialUpdateOp> fieldOps = new ArrayList<>();
            for (UpsertReq.FieldPartialUpdateOp fieldOp : requestFieldOps) {
                if (fieldOp == null) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Field op cannot be null");
                }
                if (StringUtils.isEmpty(fieldOp.getFieldName())) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Field op field name cannot be empty");
                }
                if (fieldOp.getOpType() == null) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Field op type cannot be null");
                }
                fieldOps.add(FieldPartialUpdateOp.newBuilder()
                        .setFieldName(fieldOp.getFieldName())
                        .setOp(convertFieldOpType(fieldOp.getOpType()))
                        .build());
            }
            return fieldOps;
        }

        private static FieldPartialUpdateOp.OpType convertFieldOpType(UpsertReq.FieldPartialUpdateOp.OpType opType) {
            switch (opType) {
                case REPLACE:
                    return FieldPartialUpdateOp.OpType.REPLACE;
                case ARRAY_APPEND:
                    return FieldPartialUpdateOp.OpType.ARRAY_APPEND;
                case ARRAY_REMOVE:
                    return FieldPartialUpdateOp.OpType.ARRAY_REMOVE;
                default:
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Unsupported field op type: " + opType);
            }
        }

        private void addFieldsData(FieldData value) {
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

        private void fillFieldsData(UpsertReq requestParam, DescribeCollectionResp descColl) {
            String partitionName = requestParam.getPartitionName();
            if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields, requestParam.isPartialUpdate());
        }

        private void fillFieldsData(InsertReq requestParam, DescribeCollectionResp descColl) {
            String partitionName = requestParam.getPartitionName();
            if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields, false);
        }

        private static String combineStructFieldName(String structName, String subFieldName) {
            return String.format("%s[%s]", structName, subFieldName);
        }

        private void checkAndSetRowData(DescribeCollectionResp descColl, List<JsonObject> rows,
                                        boolean partialUpdate) {
            CreateCollectionReq.CollectionSchema collectionSchema = descColl.getCollectionSchema();
            List<CreateCollectionReq.Function> functionsList = collectionSchema.getFunctionList();
            Set<String> outputFieldNames = new HashSet<>();
            for (CreateCollectionReq.Function function : functionsList) {
                outputFieldNames.addAll(function.getOutputFieldNames());
            }

            List<CreateCollectionReq.FieldSchema> normalFields = collectionSchema.getFieldSchemaList();
            List<CreateCollectionReq.StructFieldSchema> structFields = collectionSchema.getStructFields();
            boolean isUpsert = upsertBuilder != null;
            List<CreateCollectionReq.FieldSchema> inputFields = new ArrayList<>();
            Set<String> inputFieldNames = new HashSet<>();
            Set<String> structFieldNames = new HashSet<>();
            structFields.forEach((schema) -> structFieldNames.add(schema.getName()));

            for (JsonObject row : rows) {
                if (row == null) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "The row data cannot be null.");
                }
            }

            CreateCollectionReq.FieldSchema providedAutoIdField = null;
            for (CreateCollectionReq.FieldSchema field : normalFields) {
                boolean isProvidedAutoId = isProvidedAutoId(field, rows, isUpsert);
                if (isInputField(field, isUpsert, outputFieldNames)) {
                    inputFields.add(field);
                    inputFieldNames.add(field.getName());
                } else if (isProvidedAutoId) {
                    providedAutoIdField = field;
                }
            }
            if (providedAutoIdField != null) {
                inputFields.add(providedAutoIdField);
                inputFieldNames.add(providedAutoIdField.getName());
            }

            // 1. for normal fields, InsertDataInfo is a list of object or list of list, for example:
            //      Int64Field, InsertDataInfo is a List<Long>
            //      FloatVectorField, InsertDataInfo is a List<List<Float>>
            // the normalInsertData typically looks like:
            //      {
            //          "id": List<Long>,
            //          "vector": List<List<Float>>
            //      }
            //
            // 2. for struct fields, InsertDataInfo is list of list or 3-layer list, for example:
            //      A struct field named "struct1" has a sub-field "sub1" type is Varchar and a sub-field "sub2" type is FloatVector
            //      for the sub-field "sub1", InsertDataInfo is a List<List<String>>
            //      for the sub-field "sub2", InsertDataInfo is a List<List<List<Float>>>
            // the structInsertData stores all sub-fields of all struct fields, typically looks like:
            //      {
            //          "sub1 of struct1": List<List<String>>,
            //          "sub2 of struct1": List<List<List<Float>>>,
            //          "sub3 of struct2": List<List<Integer>>,
            //          "sub4 of struct2": List<List<List<Float>>>
            //       }
            Map<String, InsertDataInfo> normalInsertData = new HashMap<>();
            Map<String, InsertDataInfo> structInsertData = new HashMap<>();
            Map<String, Integer> structFieldCounts = new HashMap<>();
            InsertDataInfo insertDynamicDataInfo = new InsertDataInfo(
                    CreateCollectionReq.FieldSchema.builder()
                            .name(Constant.DYNAMIC_FIELD_NAME)
                            .dataType(io.milvus.v2.common.DataType.JSON)
                            .build(),
                    new LinkedList<>());
            for (JsonObject row : rows) {
                for (String rowFieldName : row.keySet()) {
                    if (outputFieldNames.contains(rowFieldName)) {
                        throw new DataNotMatchException(
                                String.format("The function output field: %s cannot be provided.", rowFieldName));
                    }
                    String structFieldName = isUpsert
                            ? matchStructSubFieldName(rowFieldName, structFields)
                            : null;
                    if (structFieldName != null) {
                        String message = partialUpdate
                                ? String.format("Partial struct update is unsupported for struct sub-field `%s`; "
                                + "update the whole struct field `%s` instead", rowFieldName, structFieldName)
                                : String.format("Struct sub-field `%s` cannot be used as a top-level field; "
                                + "write the whole struct field `%s` instead", rowFieldName, structFieldName);
                        throw new DataNotMatchException(message);
                    }
                    if (!inputFieldNames.contains(rowFieldName)
                            && !structFieldNames.contains(rowFieldName)
                            && !collectionSchema.isEnableDynamicField()) {
                        throw new DataNotMatchException(
                                String.format("The field: %s is not defined in the collection schema.", rowFieldName));
                    }
                }

                // check and store value of normal fields into InsertDataInfo
                for (CreateCollectionReq.FieldSchema field : inputFields) {
                    processNormalFieldValues(row, field, normalInsertData, partialUpdate);
                }

                // check and store value of struct fields into InsertDataInfo
                for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                    if (processStructFieldValues(row, structField, structInsertData, partialUpdate)) {
                        structFieldCounts.merge(structField.getName(), 1, Integer::sum);
                    }
                }

                // store dynamic fields into InsertDataInfo
                if (collectionSchema.isEnableDynamicField()) {
                    JsonObject dynamicField = new JsonObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!inputFieldNames.contains(rowFieldName) && !structFieldNames.contains(rowFieldName)) {
                            dynamicField.add(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.data.add(dynamicField);
                }
            }

            if (partialUpdate) {
                int rowCount = rows.size();
                boolean hasInvalidFieldCount = normalInsertData.values().stream()
                        .map(insertDataInfo -> insertDataInfo.data.size())
                        .filter(count -> count > 0)
                        .anyMatch(count -> count != rowCount)
                        || structFieldCounts.values().stream()
                        .filter(count -> count > 0)
                        .anyMatch(count -> count != rowCount)
                        || (collectionSchema.isEnableDynamicField()
                            && !insertDynamicDataInfo.data.isEmpty()
                            && insertDynamicDataInfo.data.size() != rowCount);
                if (hasInvalidFieldCount) {
                    throw new DataNotMatchException(
                            "The number of values for each field must be the same for partial update.");
                }
            }

            // convert normal fields data from InsertDataInfo into grpc FieldData
            for (CreateCollectionReq.FieldSchema field : inputFields) {
                InsertDataInfo insertDataInfo = normalInsertData.get(field.getName());
                if (insertDataInfo == null) {
                    continue;
                }
                this.addFieldsData(genRowFieldData(insertDataInfo.field, insertDataInfo.data, false));
            }

            // convert struct fields data from InsertDataInfo into grpc FieldData
            for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                if (!structFieldCounts.containsKey(structField.getName())) {
                    continue;
                }
                StructArrayField.Builder structBuilder = StructArrayField.newBuilder();
                for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                    String combineName = combineStructFieldName(structField.getName(), field.getName());
                    InsertDataInfo insertDataInfo = structInsertData.get(combineName);
                    FieldData grpcField;
                    try {
                        grpcField = DataUtils.genStructSubFieldData(field, insertDataInfo.data);
                    } catch (DataNotMatchException e) {
                        throw e;
                    } catch (MilvusClientException | ParamException e) {
                        throw new DataNotMatchException(e.getMessage(), e);
                    }
                    structBuilder.addFields(grpcField);
                }

                FieldData.Builder fieldDataBuilder = FieldData.newBuilder();
                this.addFieldsData(fieldDataBuilder
                        .setFieldName(structField.getName())
                        .setType(DataType.ArrayOfStruct)
                        .setStructArrays(structBuilder.build())
                        .build());
            }

            // convert dynamic field data from InsertDataInfo into grpc FieldData
            if (collectionSchema.isEnableDynamicField()) {
                this.addFieldsData(genRowFieldData(insertDynamicDataInfo.field, insertDynamicDataInfo.data, true));
            }
        }

        private static FieldData genRowFieldData(CreateCollectionReq.FieldSchema field, List<?> objects,
                                                 boolean isDynamic) {
            try {
                return DataUtils.genFieldData(field, objects, isDynamic);
            } catch (ParamException e) {
                throw new DataNotMatchException(e.getMessage(), e);
            }
        }

        private static boolean isInputField(CreateCollectionReq.FieldSchema field, boolean isUpsert,
                                            Set<String> outputFieldNames) {
            return (!Boolean.TRUE.equals(field.getAutoID()) || isUpsert)
                    && !outputFieldNames.contains(field.getName());
        }

        /**
         * Determines whether an insert batch explicitly provides values for an auto-ID primary key,
         * so the field can be included in the generated request. The field must be present in every
         * row or omitted from every row; mixing the two forms in one batch is invalid.
         * Milvus 2.6.3 or later accepts the provided values only when the collection property
         * {@code allow_insert_auto_id} is set to {@code true}.
         *
         * @return {@code true} when every row provides the auto-ID primary key, otherwise {@code false}
         * @throws DataNotMatchException when only some rows provide the auto-ID primary key
         */
        private static boolean isProvidedAutoId(CreateCollectionReq.FieldSchema field, List<JsonObject> rows,
                                                boolean isUpsert) {
            if (isUpsert
                    || !Boolean.TRUE.equals(field.getIsPrimaryKey())
                    || !Boolean.TRUE.equals(field.getAutoID())
                    || rows.isEmpty()) {
                return false;
            }

            int providedCount = 0;
            for (JsonObject row : rows) {
                if (row.has(field.getName())) {
                    providedCount++;
                }
            }
            if (providedCount > 0 && providedCount < rows.size()) {
                String msg = String.format("The auto-ID primary key field: %s must be provided for all rows "
                        + "or omitted for all rows.", field.getName());
                throw new DataNotMatchException(msg);
            }
            return providedCount == rows.size();
        }

        private static String matchStructSubFieldName(String fieldName,
                                                      List<CreateCollectionReq.StructFieldSchema> structFields) {
            for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                String prefix = structField.getName() + "[";
                if (!fieldName.startsWith(prefix) || !fieldName.endsWith("]")) {
                    continue;
                }
                String subFieldName = fieldName.substring(prefix.length(), fieldName.length() - 1);
                for (CreateCollectionReq.FieldSchema subField : structField.getFields()) {
                    if (subField.getName().equals(subFieldName)) {
                        return structField.getName();
                    }
                }
            }
            return null;
        }

        private void processNormalFieldValues(JsonObject row, CreateCollectionReq.FieldSchema field,
                                              Map<String, InsertDataInfo> nameInsertInfo, boolean partialUpdate) {
            String fieldName = field.getName();
            InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, new InsertDataInfo(field, new LinkedList<>()));

            JsonElement fieldData = row.get(fieldName);
            if (fieldData == null) {
                // in v2.6.1 support partial update, user can input partial fields
                if (partialUpdate) {
                    if (Boolean.TRUE.equals(field.getIsPrimaryKey())) {
                        String msg = String.format("The primary key field: %s is not provided.", fieldName);
                        throw new DataNotMatchException(msg);
                    }
                    return;
                }

                // if the field doesn't have default value, require user provide the value
                if (!field.getIsNullable() && field.getDefaultValue() == null) {
                    String msg = String.format("The field: %s is not provided.", fieldName);
                    throw new DataNotMatchException(msg);
                }

                fieldData = JsonNull.INSTANCE;
            }

            // from v2.4.10, milvus allows upsert for auto-id pk, no need to check for upsert action
            // from v2.6.3, user can insert pk value even when auto-id is true if the collection
            // has "allow_insert_auto_id" property, no need to check for insert/upsert action.
//            if (field.getAutoID() == Boolean.TRUE && insertBuilder != null) {
//                String msg = String.format("The primary key: %s is auto generated, no need to input.", fieldName);
//                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
//            }

            // store the value into InsertDataInfo
            Object fieldValue = checkRowFieldValue(field, fieldData);
            insertDataInfo.data.add(fieldValue);
            nameInsertInfo.put(fieldName, insertDataInfo);
        }

        private static Object checkRowFieldValue(CreateCollectionReq.FieldSchema field, JsonElement fieldData) {
            try {
                return DataUtils.checkFieldValue(field, fieldData);
            } catch (RuntimeException e) {
                throw new DataNotMatchException(e.getMessage(), e);
            }
        }

        private boolean processStructFieldValues(JsonObject row, CreateCollectionReq.StructFieldSchema structField,
                                                 Map<String, InsertDataInfo> nameInsertInfo, boolean partialUpdate) {
            String structName = structField.getName();
            JsonElement rowFieldData = row.get(structName);
            if (rowFieldData == null) {
                if (partialUpdate) {
                    return false;
                }
                String msg = String.format("The field: %s is not provided.", structName);
                throw new DataNotMatchException(msg);
            }
            if (!rowFieldData.isJsonArray()) {
                String msg = String.format("The value of struct field: %s is not a JSON array.", structName);
                throw new DataNotMatchException(msg);
            }

            initializeStructFieldData(structField, nameInsertInfo);
            Set<String> expectedFieldNames = new HashSet<>();
            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                expectedFieldNames.add(field.getName());
            }

            JsonArray structs = rowFieldData.getAsJsonArray();
            structs.forEach((element) -> {
                if (!element.isJsonObject()) {
                    String msg = String.format("The element of struct field: %s is not a JSON dict.", structName);
                    throw new DataNotMatchException(msg);
                }

                JsonObject struct = element.getAsJsonObject();
                Set<String> missingFields = new HashSet<>(expectedFieldNames);
                missingFields.removeAll(struct.keySet());
                if (!missingFields.isEmpty()) {
                    String msg = String.format("The struct field: %s is missing required fields: %s.",
                            structName, missingFields);
                    throw new DataNotMatchException(msg);
                }
                Set<String> unexpectedFields = new HashSet<>(struct.keySet());
                unexpectedFields.removeAll(expectedFieldNames);
                if (!unexpectedFields.isEmpty()) {
                    String msg = String.format("The struct field: %s has unexpected fields: %s.",
                            structName, unexpectedFields);
                    throw new DataNotMatchException(msg);
                }
                for (String fieldName : expectedFieldNames) {
                    if (struct.get(fieldName).isJsonNull()) {
                        String msg = String.format("The %s of struct field: %s cannot be null.",
                                fieldName, structName);
                        throw new DataNotMatchException(msg);
                    }
                }
            });

            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                String subFieldName = field.getName();
                InsertDataInfo insertDataInfo = nameInsertInfo.get(combineStructFieldName(structName, subFieldName));
                List<Object> columnData = new ArrayList<>();
                structs.forEach((element) -> {
                    JsonObject struct = element.getAsJsonObject();
                    JsonElement fieldData = struct.get(subFieldName);
                    Object fieldValue = checkRowFieldValue(field, fieldData);
                    columnData.add(fieldValue);
                });
                insertDataInfo.data.add(columnData);
            }
            return true;
        }

        private void initializeStructFieldData(CreateCollectionReq.StructFieldSchema structField,
                                               Map<String, InsertDataInfo> nameInsertInfo) {
            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                String combineName = combineStructFieldName(structField.getName(), field.getName());
                InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(
                        combineName, new InsertDataInfo(field, new LinkedList<>()));
                nameInsertInfo.put(combineName, insertDataInfo);
            }
        }
    }

    public static class InsertDataInfo {
        public CreateCollectionReq.FieldSchema field;
        public LinkedList<Object> data;

        public InsertDataInfo(CreateCollectionReq.FieldSchema field, LinkedList<Object> data) {
            this.field = field;
            this.data = data;
        }
    }

    private static FieldData genStructSubFieldData(CreateCollectionReq.FieldSchema fieldSchema, List<?> objects) {
        DataType dataType = ConvertUtils.toProtoDataType(fieldSchema.getDataType());
        String fieldName = fieldSchema.getName();
        FieldData.Builder builder = FieldData.newBuilder().setFieldName(fieldName);

        if (ParamUtils.isVectorDataType(dataType)) {
            VectorArray vectorArr = genVectorArray(dataType, objects, fieldSchema.getDimension());
            if (vectorArr.getDim() > 0 && vectorArr.getDim() != fieldSchema.getDimension()) {
                String msg = String.format("Dimension mismatch for field %s, expected: %d, actual: %d",
                        fieldName, fieldSchema.getDimension(), vectorArr.getDim());
                throw new DataNotMatchException(msg);
            }
            return builder.setType(DataType.ArrayOfVector)
                    .setVectors(VectorField.newBuilder()
                            .setVectorArray(vectorArr)
                            .setDim(fieldSchema.getDimension())
                            .build())
                    .build();
        } else {
            if (fieldSchema.getIsNullable() || fieldSchema.getDefaultValue() != null) {
                List<Object> tempObjects = new ArrayList<>();
                for (Object obj : objects) {
                    builder.addValidData(obj != null);
                    if (obj != null) {
                        tempObjects.add(obj);
                    }
                }
                objects = tempObjects;
            }

            ScalarField scalarField = ParamUtils.genScalarField(DataType.Array, dataType, objects);
            return builder.setType(DataType.Array).setScalars(scalarField).build();
        }
    }

    @SuppressWarnings("unchecked")
    public static VectorArray genVectorArray(DataType dataType, List<?> objects, int dim) {
        VectorArray.Builder builder = VectorArray.newBuilder().setElementType(dataType).setDim(dim);
        switch (dataType) {
            case FloatVector:
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector:
            case Int8Vector: {
                // for FloatVector, objects is List<List<List<Float>>>
                // for others, objects is List<List<List<ByteBuffer>>>
                for (Object object : objects) {
                    if (!(object instanceof List)) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Input value is not List<> for type: " + dataType.name());
                    }

                    List<?> listOfList = (List<?>) object;
                    VectorField vf = ParamUtils.genVectorField(dataType, listOfList);
                    if (listOfList.isEmpty()) {
                        vf = vf.toBuilder().setDim(dim).build();
                    }
                    if (vf.getDim() != dim) {
                        String msg = String.format("Dimension mismatch for vector field, schema dimension: %d, actual dimension: %d",
                                dim, vf.getDim());
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                    }
                    builder.addData(vf);
                }
                return builder.build();
            }
            default:
                // so far, struct field only supports FloatVector/BinaryVector/Float16Vector/BFloat16Vector/Int8Vector
                String msg = String.format("Illegal vector dataType %s for struct field", dataType.name());
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
        }
    }

    public DeleteRequest ConvertToGrpcDeleteRequest(DeleteReq request) {
        DeleteRequest.Builder builder = DeleteRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName())
                .setExpr(request.getFilter());
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            Map<String, Object> filterTemplateValues = request.getFilterTemplateValues();
            filterTemplateValues.forEach((key, value) -> {
                builder.putExprTemplateValues(key, VectorUtils.deduceAndCreateTemplateValue(value));
            });
        }
        String dbName = request.getDatabaseName();
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        return builder.build();
    }

    private static FieldData genFieldData(CreateCollectionReq.FieldSchema field, List<?> objects, boolean isDynamic) {
        String fieldName = field.getName();
        DataType dataType = ConvertUtils.toProtoDataType(field.getDataType());
        DataType elementType = ConvertUtils.toProtoDataType(field.getElementType());
        boolean isNullable = field.getIsNullable();
        Object defaultVal = field.getDefaultValue();
        int dimension = field.getDimension() == null ? 0 : field.getDimension();
        return ParamUtils.genFieldData(
                fieldName, dataType, elementType, isNullable, defaultVal, objects, isDynamic, dimension);
    }

    public static Object checkFieldValue(CreateCollectionReq.FieldSchema field, JsonElement fieldData) {
        DataType dataType = ConvertUtils.toProtoDataType(field.getDataType());
        DataType elementType = ConvertUtils.toProtoDataType(field.getElementType());
        int dim = field.getDimension() == null ? 0 : field.getDimension();
        int maxLength = field.getMaxLength() == null ? 0 : field.getMaxLength();
        int maxCapacity = field.getMaxCapacity() == null ? 0 : field.getMaxCapacity();
        return ParamUtils.checkFieldValue(field.getName(), dataType, elementType, dim,
                maxLength, maxCapacity, field.getIsNullable(), field.getDefaultValue(), fieldData);
    }
}
