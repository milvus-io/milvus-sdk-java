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
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
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
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Upsert).build();
            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setPartialUpdate(requestParam.isPartialUpdate())
                    .setNumRows(requestParam.getData().size());
            if (StringUtils.isNotEmpty(dbName)) {
                upsertBuilder.setDbName(dbName);
            }
            insertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return upsertBuilder.build();
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

        private static boolean hasPartitionKey(DescribeCollectionResp descColl) {
            CreateCollectionReq.CollectionSchema collectionSchema = descColl.getCollectionSchema();
            List<CreateCollectionReq.FieldSchema> fieldsList = collectionSchema.getFieldSchemaList();
            for (CreateCollectionReq.FieldSchema field : fieldsList) {
                if (field.getIsPartitionKey() == Boolean.TRUE) {
                    return true;
                }
            }
            return false;
        }

        private void fillFieldsData(UpsertReq requestParam, DescribeCollectionResp descColl) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            if (hasPartitionKey(descColl)) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields, requestParam.isPartialUpdate());
        }

        private void fillFieldsData(InsertReq requestParam, DescribeCollectionResp descColl) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            if (hasPartitionKey(descColl)) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields, false);
        }

        private static String combineStructFieldName(String structName, String subFieldName) {
            return String.format("%s[%s]", structName, subFieldName);
        }

        private void checkAndSetRowData(DescribeCollectionResp descColl, List<JsonObject> rows, boolean partialUpdate) {
            CreateCollectionReq.CollectionSchema collectionSchema = descColl.getCollectionSchema();
            List<CreateCollectionReq.Function> functionsList = collectionSchema.getFunctionList();
            List<String> outputFieldNames = new ArrayList<>();
            for (CreateCollectionReq.Function function : functionsList) {
                outputFieldNames.addAll(function.getOutputFieldNames());
            }

            List<CreateCollectionReq.FieldSchema> normalFields = collectionSchema.getFieldSchemaList();
            List<CreateCollectionReq.StructFieldSchema> structFields = collectionSchema.getStructFields();
            List<String> allFieldNames = new ArrayList<>();
            normalFields.forEach((schema) -> allFieldNames.add(schema.getName()));
            structFields.forEach((schema) -> allFieldNames.add(schema.getName()));

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
            InsertDataInfo insertDynamicDataInfo = new InsertDataInfo(
                    CreateCollectionReq.FieldSchema.builder()
                            .name(Constant.DYNAMIC_FIELD_NAME)
                            .dataType(io.milvus.v2.common.DataType.JSON)
                            .build(),
                    new LinkedList<>());
            for (JsonObject row : rows) {
                // check and store value of normal fields into InsertDataInfo
                for (CreateCollectionReq.FieldSchema field : normalFields) {
                    processNormalFieldValues(row, field, outputFieldNames, normalInsertData, partialUpdate);
                }

                // check and store value of struct fields into InsertDataInfo
                for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                    processStructFieldValues(row, structField, structInsertData);
                }

                // store dynamic fields into InsertDataInfo
                if (collectionSchema.isEnableDynamicField()) {
                    JsonObject dynamicField = new JsonObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!allFieldNames.contains(rowFieldName)) {
                            dynamicField.add(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.data.add(dynamicField);
                }
            }

            // convert normal fields data from InsertDataInfo into grpc FieldData
            for (String fieldNameKey : normalInsertData.keySet()) {
                InsertDataInfo insertDataInfo = normalInsertData.get(fieldNameKey);
                this.addFieldsData(DataUtils.genFieldData(insertDataInfo.field, insertDataInfo.data, false));
            }

            // convert struct fields data from InsertDataInfo into grpc FieldData
            for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                StructArrayField.Builder structBuilder = StructArrayField.newBuilder();
                for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                    String combineName = combineStructFieldName(structField.getName(), field.getName());
                    InsertDataInfo insertDataInfo = structInsertData.get(combineName);
                    FieldData grpcField = DataUtils.genStructSubFieldData(field, insertDataInfo.data);
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
                this.addFieldsData(DataUtils.genFieldData(insertDynamicDataInfo.field, insertDynamicDataInfo.data, true));
            }
        }

        private void processNormalFieldValues(JsonObject row, CreateCollectionReq.FieldSchema field,
                                              List<String> outputFieldNames,
                                              Map<String, InsertDataInfo> nameInsertInfo, boolean partialUpdate) {
            String fieldName = field.getName();
            InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, new InsertDataInfo(field, new LinkedList<>()));

            JsonElement fieldData = row.get(fieldName);
            if (fieldData == null) {
                // if the field is auto-id, no need to provide value
                if (field.getAutoID() == Boolean.TRUE) {
                    return;
                }

                // if the field is an output field of doc-in-doc-out, no need to provide value
                if (outputFieldNames.contains(fieldName)) {
                    return;
                }

                // in v2.6.1 support partial update, user can input partial fields
                if (partialUpdate) {
                    return;
                }

                // if the field doesn't have default value, require user provide the value
                if (!field.getIsNullable() && field.getDefaultValue() == null) {
                    String msg = String.format("The field: %s is not provided.", fieldName);
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }

                fieldData = JsonNull.INSTANCE;
            }

            // from v2.4.10, milvus allows upsert for auto-id pk, no need to check for upsert action
            if (field.getAutoID() == Boolean.TRUE && insertBuilder != null) {
                String msg = String.format("The primary key: %s is auto generated, no need to input.", fieldName);
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
            }

            // store the value into InsertDataInfo
            Object fieldValue = DataUtils.checkFieldValue(field, fieldData);
            insertDataInfo.data.add(fieldValue);
            nameInsertInfo.put(fieldName, insertDataInfo);
        }

        private void processStructFieldValues(JsonObject row, CreateCollectionReq.StructFieldSchema structField,
                                              Map<String, InsertDataInfo> nameInsertInfo) {
            String structName = structField.getName();
            JsonElement rowFieldData = row.get(structName);
            if (rowFieldData == null) {
                String msg = String.format("The struct field: %s is not provided.", structName);
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
            }
            if (!rowFieldData.isJsonArray()) {
                String msg = String.format("The value of struct field: %s is not a JSON array.", structName);
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
            }

            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                String combineName = combineStructFieldName(structName, field.getName());
                InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(combineName, new InsertDataInfo(field, new LinkedList<>()));
                nameInsertInfo.put(combineName, insertDataInfo);
            }

            JsonArray structs = rowFieldData.getAsJsonArray();
            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                String subFieldName = field.getName();
                InsertDataInfo insertDataInfo = nameInsertInfo.get(combineStructFieldName(structName, subFieldName));
                List<Object> columnData = new ArrayList<>();
                structs.forEach((element) -> {
                    if (!element.isJsonObject()) {
                        String msg = String.format("The element of struct field: %s is not a JSON dict.", structName);
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                    }

                    JsonObject struct = element.getAsJsonObject();
                    JsonElement fieldData = struct.get(subFieldName);
                    if (fieldData == null) {
                        String msg = String.format("The %s of struct field: %s is not provided.", subFieldName, structName);
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                    }

                    Object fieldValue = DataUtils.checkFieldValue(field, fieldData);
                    columnData.add(fieldValue);
                });
                insertDataInfo.data.add(columnData);
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
        if (objects == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Cannot generate FieldData from null object");
        }
        DataType dataType = ConvertUtils.toProtoDataType(fieldSchema.getDataType());
        String fieldName = fieldSchema.getName();
        FieldData.Builder builder = FieldData.newBuilder().setFieldName(fieldName);

        if (ParamUtils.isVectorDataType(dataType)) {
            VectorArray vectorArr = genVectorArray(dataType, objects);
            if (vectorArr.getDim() > 0 && vectorArr.getDim() != fieldSchema.getDimension()) {
                String msg = String.format("Dimension mismatch for field %s, expected: %d, actual: %d",
                        fieldName, fieldSchema.getDimension(), vectorArr.getDim());
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
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
    public static VectorArray genVectorArray(DataType dataType, List<?> objects) {
        VectorArray.Builder builder = VectorArray.newBuilder().setElementType(dataType);
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
                    if (listOfList.isEmpty()) {
                        // struct field value is empty, fill the VectorArray with zero-dim vectors?
                        builder.addData(VectorField.newBuilder().build());
                        continue;
                    }

                    VectorField vf = ParamUtils.genVectorField(dataType, listOfList);
                    if (vf.getDim() == 0) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Vector cannot be empty list");
                    }
                    if (builder.getDataCount() == 0) {
                        builder.setDim(vf.getDim());
                    } else if (builder.getDim() != vf.getDim()) {
                        String msg = String.format("Dimension mismatch for vector field, the first dimension: %d, mismatched: %d",
                                builder.getDim(), vf.getDim());
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
        return ParamUtils.genFieldData(fieldName, dataType, elementType, isNullable, defaultVal, objects, isDynamic);
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
