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

/**
 * Helper for converting V2 insert/upsert/delete request data into gRPC requests. Validates
 * row data against the collection schema and generates gRPC FieldData for normal, struct,
 * and dynamic fields.
 */


public class DataUtils {

    /**
     * Wrapper holding the gRPC insert/upsert request builders while converting V2 row data.
     */


    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;
        private List<UpsertReq.FieldPartialUpdateOp> requestFieldOps;

        /**
         * Converts the given V2 insert request into a gRPC insert request, validating the row
         * data against the collection schema.
         *
         * @param requestParam the V2 insert request
         * @param descColl the described collection schema
         * @return the built gRPC insert request
         */


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
            if (StringUtils.isNotEmpty(requestParam.getRlsPrincipal())) {
                insertBuilder.setRlsPrincipal(requestParam.getRlsPrincipal());
            }
            if (requestParam.getSkipRls() != null) {
                insertBuilder.setSkipRls(requestParam.getSkipRls());
            }
            upsertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return insertBuilder.build();
        }

        /**
         * Converts the given V2 upsert request into a gRPC upsert request, validating the row
         * data against the collection schema.
         *
         * @param requestParam the V2 upsert request
         * @param descColl the described collection schema
         * @return the built gRPC upsert request
         */


        public UpsertRequest convertGrpcUpsertRequest(UpsertReq requestParam, DescribeCollectionResp descColl) {
            String dbName = requestParam.getDatabaseName();
            String collectionName = requestParam.getCollectionName();
            this.requestFieldOps = requestParam.getFieldOps();

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
            if (StringUtils.isNotEmpty(requestParam.getRlsPrincipal())) {
                upsertBuilder.setRlsPrincipal(requestParam.getRlsPrincipal());
            }
            if (requestParam.getSkipRls() != null) {
                upsertBuilder.setSkipRls(requestParam.getSkipRls());
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
            // a struct field can carry at most one PATH_REPLACE op, and a PATH_REPLACE op
            // cannot be combined with any other op on the same field; pathReplaceChild()
            // encodes only the PATH_REPLACE child's data, so any second op on that field
            // would receive row data that does not match its semantics (pymilvus keys
            // field ops by field name and cannot express two ops on one field)
            Set<String> pathReplaceFields = new HashSet<>();
            Set<String> nonPathReplaceFields = new HashSet<>();
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
                FieldPartialUpdateOp.Builder builder = FieldPartialUpdateOp.newBuilder()
                        .setFieldName(fieldOp.getFieldName())
                        .setOp(convertFieldOpType(fieldOp.getOpType()));
                if (fieldOp.getOpType() == UpsertReq.FieldPartialUpdateOp.OpType.PATH_REPLACE) {
                    if (StringUtils.isEmpty(fieldOp.getPath())) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "PATH_REPLACE requires a path for field: " + fieldOp.getFieldName());
                    }
                    if (!pathReplaceFields.add(fieldOp.getFieldName())) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "Only one PATH_REPLACE op is supported per field: " + fieldOp.getFieldName());
                    }
                    if (nonPathReplaceFields.contains(fieldOp.getFieldName())) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "PATH_REPLACE cannot be combined with another op on the same field: " + fieldOp.getFieldName());
                    }
                    builder.setPath(fieldOp.getPath());
                } else {
                    if (pathReplaceFields.contains(fieldOp.getFieldName())) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "PATH_REPLACE cannot be combined with another op on the same field: " + fieldOp.getFieldName());
                    }
                    if (StringUtils.isNotEmpty(fieldOp.getPath())) {
                        // the proto contract and pymilvus both require an empty path for
                        // non-PATH_REPLACE ops; a typo must not silently degrade to whole-field
                        // REPLACE semantics
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "path is only supported for PATH_REPLACE on field: " + fieldOp.getFieldName());
                    }
                    nonPathReplaceFields.add(fieldOp.getFieldName());
                }
                fieldOps.add(builder.build());
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
                case PATH_REPLACE:
                    return FieldPartialUpdateOp.OpType.PATH_REPLACE;
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

        /**
         * Returns the child sub-field name targeted by a PATH_REPLACE op on the given struct
         * field, or {@code null} when the op addresses a whole element ({@code [index]}) or
         * when no PATH_REPLACE op targets the field.
         *
         * @param structName the struct field name
         * @return the child sub-field name for a {@code [index][child]} path, otherwise {@code null}
         * @throws MilvusClientException when the path is malformed or the child is unknown
         */
        private String pathReplaceChild(String structName) {
            if (requestFieldOps == null) {
                return null;
            }
            UpsertReq.FieldPartialUpdateOp pathOp = null;
            for (UpsertReq.FieldPartialUpdateOp op : requestFieldOps) {
                if (op != null && structName.equals(op.getFieldName())
                        && op.getOpType() == UpsertReq.FieldPartialUpdateOp.OpType.PATH_REPLACE) {
                    pathOp = op;
                    break;
                }
            }
            if (pathOp == null) {
                return null;
            }
            String path = pathOp.getPath();
            if (StringUtils.isBlank(path)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        String.format("PATH_REPLACE requires a path for struct field '%s'", structName));
            }
            // strictly match [index] or [index][child], aligned with pymilvus;
            // a typo must not silently fall back to the whole-element interpretation
            java.util.regex.Matcher matcher = SchemaUtils.STRUCT_PATH_REPLACE.matcher(path);
            if (!matcher.matches()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        String.format("PATH_REPLACE on struct '%s' requires [index] or [index][child], got: '%s'",
                                structName, path));
            }
            // group 1 is the optional child segment
            return matcher.group(1);
        }

        /**
         * Returns whether a PATH_REPLACE op targets the given struct field.
         *
         * @param structName the struct field name
         * @return {@code true} if a PATH_REPLACE op targets the field
         */
        private boolean hasPathReplace(String structName) {
            if (requestFieldOps == null) {
                return false;
            }
            for (UpsertReq.FieldPartialUpdateOp op : requestFieldOps) {
                if (op != null && structName.equals(op.getFieldName())
                        && op.getOpType() == UpsertReq.FieldPartialUpdateOp.OpType.PATH_REPLACE) {
                    return true;
                }
            }
            return false;
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
                String pathReplaceChild = pathReplaceChild(structField.getName());
                List<CreateCollectionReq.FieldSchema> subFields = structField.getFields();
                if (pathReplaceChild != null) {
                    List<CreateCollectionReq.FieldSchema> onlyChild = new ArrayList<>();
                    for (CreateCollectionReq.FieldSchema field : subFields) {
                        if (pathReplaceChild.equals(field.getName())) {
                            onlyChild.add(field);
                            break;
                        }
                    }
                    subFields = onlyChild;
                }
                for (CreateCollectionReq.FieldSchema field : subFields) {
                    String combineName = combineStructFieldName(structField.getName(), field.getName());
                    InsertDataInfo insertDataInfo = structInsertData.get(combineName);
                    FieldData grpcField;
                    try {
                        grpcField = DataUtils.genStructSubFieldData(field, insertDataInfo.data,
                                Boolean.TRUE.equals(structField.getNullable()));
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
            String pathReplaceChild = pathReplaceChild(structName);
            boolean isPathReplace = hasPathReplace(structName);
            JsonElement rowFieldData = row.get(structName);
            if (isPathReplace && (rowFieldData == null || rowFieldData.isJsonNull())) {
                // a PATH_REPLACE operand must be a single-element array; a null parent is rejected
                // so the operation does not silently degrade into a null struct row
                String msg = String.format("PATH_REPLACE requires an operand for struct field '%s' in every row, "
                        + "got a null or missing value", structName);
                throw new DataNotMatchException(msg);
            }
            if (rowFieldData == null) {
                if (partialUpdate) {
                    // a missing key in a partial update means "leave the field untouched"
                    return false;
                }
                if (!Boolean.TRUE.equals(structField.getNullable())) {
                    throw new DataNotMatchException(String.format("The field: %s is not provided.", structName));
                }
                addNullStructRow(structField, nameInsertInfo);
                return true;
            }
            if (rowFieldData.isJsonNull()) {
                if (!Boolean.TRUE.equals(structField.getNullable())) {
                    throw new DataNotMatchException(String.format("The value of struct field: %s cannot be null.", structName));
                }
                // an explicit null in a partial update nulls out the struct, like a nullable scalar field
                addNullStructRow(structField, nameInsertInfo);
                return true;
            }
            if (!rowFieldData.isJsonArray()) {
                String msg = String.format("The value of struct field: %s is not a JSON array.", structName);
                throw new DataNotMatchException(msg);
            }

            JsonArray structs = rowFieldData.getAsJsonArray();
            if (isPathReplace && structs.size() != 1) {
                String msg = String.format("PATH_REPLACE requires exactly one element for struct field '%s' in every row",
                        structName);
                throw new DataNotMatchException(msg);
            }

            // for a child path, only the targeted child sub-field participates in the request;
            // the operand element must explicitly contain the child key
            List<CreateCollectionReq.FieldSchema> subFields = structField.getFields();
            if (pathReplaceChild != null) {
                List<CreateCollectionReq.FieldSchema> onlyChild = new ArrayList<>();
                for (CreateCollectionReq.FieldSchema field : subFields) {
                    if (pathReplaceChild.equals(field.getName())) {
                        onlyChild.add(field);
                        break;
                    }
                }
                if (onlyChild.isEmpty()) {
                    String msg = String.format("PATH_REPLACE child '%s' not found in struct field '%s'",
                            pathReplaceChild, structName);
                    throw new DataNotMatchException(msg);
                }
                subFields = onlyChild;
            }

            initializeStructFieldData(structName, subFields, nameInsertInfo);
            Set<String> expectedFieldNames = new HashSet<>();
            for (CreateCollectionReq.FieldSchema field : subFields) {
                expectedFieldNames.add(field.getName());
            }

            structs.forEach((element) -> {
                if (!element.isJsonObject()) {
                    String msg = String.format("The element of struct field: %s is not a JSON dict.", structName);
                    throw new DataNotMatchException(msg);
                }

                JsonObject struct = element.getAsJsonObject();
                Set<String> unexpectedFields = new HashSet<>(struct.keySet());
                unexpectedFields.removeAll(expectedFieldNames);
                if (!unexpectedFields.isEmpty()) {
                    String msg = String.format("The struct field: %s has unexpected fields: %s.",
                            structName, unexpectedFields);
                    throw new DataNotMatchException(msg);
                }
                if (pathReplaceChild != null && !struct.has(pathReplaceChild)) {
                    String msg = String.format("PATH_REPLACE operand for struct field '%s' must explicitly contain child '%s'",
                            structName, pathReplaceChild);
                    throw new DataNotMatchException(msg);
                }
            });

            for (CreateCollectionReq.FieldSchema field : subFields) {
                String subFieldName = field.getName();
                boolean subFieldNullable = Boolean.TRUE.equals(field.getIsNullable());
                InsertDataInfo insertDataInfo = nameInsertInfo.get(combineStructFieldName(structName, subFieldName));
                List<Object> columnData = new ArrayList<>();
                structs.forEach((element) -> {
                    JsonObject struct = element.getAsJsonObject();
                    JsonElement fieldData = struct.get(subFieldName);
                    Object fieldValue;
                    if (fieldData == null || fieldData.isJsonNull()) {
                        // a nullable sub-field accepts a missing key or an explicit null
                        if (!subFieldNullable) {
                            String msg = String.format("The %s of struct field: %s cannot be null.",
                                    subFieldName, structName);
                            throw new DataNotMatchException(msg);
                        }
                        fieldValue = null;
                    } else {
                        fieldValue = checkRowFieldValue(field, fieldData);
                    }
                    columnData.add(fieldValue);
                });
                insertDataInfo.data.add(columnData);
            }
            return true;
        }

        private void initializeStructFieldData(String structName, List<CreateCollectionReq.FieldSchema> subFields,
                                                Map<String, InsertDataInfo> nameInsertInfo) {
            for (CreateCollectionReq.FieldSchema field : subFields) {
                String combineName = combineStructFieldName(structName, field.getName());
                InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(
                        combineName, new InsertDataInfo(field, new LinkedList<>()));
                nameInsertInfo.put(combineName, insertDataInfo);
            }
        }

        private void addNullStructRow(CreateCollectionReq.StructFieldSchema structField,
                                      Map<String, InsertDataInfo> nameInsertInfo) {
            initializeStructFieldData(structField.getName(), structField.getFields(), nameInsertInfo);
            for (CreateCollectionReq.FieldSchema field : structField.getFields()) {
                String combineName = combineStructFieldName(structField.getName(), field.getName());
                nameInsertInfo.get(combineName).data.add(null);
            }
        }
    }

    /**
     * Data class for {@link DataUtils}.
     */


    public static class InsertDataInfo {
        /**
         * The field schema the collected values belong to.
         */
        public CreateCollectionReq.FieldSchema field;
        /**
         * The values collected for the field across rows.
         */
        public LinkedList<Object> data;

        /**
         * Creates an {@code InsertDataInfo} for a field and the data collected for it.
         *
         * @param field the field schema
         * @param data the collected field data
         */


        public InsertDataInfo(CreateCollectionReq.FieldSchema field, LinkedList<Object> data) {
            this.field = field;
            this.data = data;
        }
    }

    private static FieldData genStructSubFieldData(CreateCollectionReq.FieldSchema fieldSchema, List<?> objects,
                                                    boolean rowNullable) {
        DataType dataType = ConvertUtils.toProtoDataType(fieldSchema.getDataType());
        String fieldName = fieldSchema.getName();
        boolean elementNullable = Boolean.TRUE.equals(fieldSchema.getIsNullable());
        FieldData.Builder builder = FieldData.newBuilder().setFieldName(fieldName);

        // JSON/Geometry/Timestamptz are not supported as struct sub-field element types
        // (aligned with pymilvus, which rejects them on insert)
        if (dataType == DataType.JSON || dataType == DataType.Geometry || dataType == DataType.Timestamptz) {
            String msg = String.format("Unsupported element type %s for struct sub-field '%s'", dataType, fieldName);
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
        }

        // split rows into per-row validity (row-level, driven by the struct's row nullable)
        // and non-null rows; a null struct row is marked false and contributes no data cell
        List<Boolean> rowValidity = new ArrayList<>();
        List<Object> nonNullRows = new ArrayList<>();
        for (Object obj : objects) {
            rowValidity.add(obj != null);
            if (obj != null) {
                nonNullRows.add(obj);
            }
        }

        if (ParamUtils.isVectorDataType(dataType)) {
            int dim = fieldSchema.getDimension() == null ? 0 : fieldSchema.getDimension();
            if (rowNullable) {
                builder.addAllValidData(rowValidity);
            }
            if (nonNullRows.isEmpty()) {
                // all struct rows are null; leave the payload unset
                return builder.setType(DataType.ArrayOfVector).build();
            }
            VectorArray.Builder vaBuilder = VectorArray.newBuilder().setElementType(dataType).setDim(dim);
            for (Object row : nonNullRows) {
                List<?> vectors = (List<?>) row;
                vaBuilder.addData(genStructVectorRow(dataType, vectors, elementNullable, dim, fieldName));
            }
            VectorField.Builder vectorsBuilder = VectorField.newBuilder().setVectorArray(vaBuilder).setDim(dim);
            if (rowNullable) {
                vectorsBuilder.addAllValidData(rowValidity);
            }
            return builder.setType(DataType.ArrayOfVector).setVectors(vectorsBuilder.build()).build();
        } else if (dataType == DataType.Array) {
            // nested Array sub-field: the outer array_data element type is ARRAY and each row
            // cell carries its own inner array_data with the leaf element type
            if (rowNullable) {
                builder.addAllValidData(rowValidity);
            }
            if (nonNullRows.isEmpty()) {
                return builder.setType(DataType.Array).build();
            }
            ArrayArray.Builder outerBuilder = ArrayArray.newBuilder().setElementType(DataType.Array);
            for (Object row : nonNullRows) {
                List<?> innerArrays = (List<?>) row;
                outerBuilder.addData(genStructNestedArrayRow(fieldSchema, innerArrays, fieldName));
            }
            ScalarField.Builder scalarsBuilder = ScalarField.newBuilder().setArrayData(outerBuilder);
            if (rowNullable) {
                scalarsBuilder.addAllValidData(rowValidity);
            }
            return builder.setType(DataType.Array).setScalars(scalarsBuilder.build()).build();
        } else {
            // scalar sub-field: the outer array_data element type is the scalar element type
            // and each row cell holds that row's element values
            if (rowNullable) {
                builder.addAllValidData(rowValidity);
            }
            if (nonNullRows.isEmpty()) {
                return builder.setType(DataType.Array).build();
            }
            ArrayArray.Builder outerBuilder = ArrayArray.newBuilder().setElementType(dataType);
            for (Object row : nonNullRows) {
                List<?> values = (List<?>) row;
                outerBuilder.addData(genStructScalarRow(dataType, values, elementNullable, fieldName));
            }
            ScalarField.Builder scalarsBuilder = ScalarField.newBuilder().setArrayData(outerBuilder);
            if (rowNullable) {
                scalarsBuilder.addAllValidData(rowValidity);
            }
            return builder.setType(DataType.Array).setScalars(scalarsBuilder.build()).build();
        }
    }

    /**
     * Encodes one struct row's scalar sub-field values into a per-row ScalarField cell.
     * When the sub-field is element-nullable, null elements are compacted and the
     * element-level validity is recorded on the cell; otherwise all values are written
     * densely without validity (a null element would be a user error, rejected earlier).
     *
     * @param elementType the proto scalar element type
     * @param values the row's element values (may contain nulls when element-nullable)
     * @param elementNullable whether the sub-field permits null elements
     * @param name the sub-field name (for error messages)
     * @return the encoded per-row ScalarField cell
     */
    private static ScalarField genStructScalarRow(DataType elementType, List<?> values,
                                                  boolean elementNullable, String name) {
        if (!elementNullable) {
            for (Object value : values) {
                if (value == null) {
                    String msg = String.format("Array sub-field '%s' does not allow null elements", name);
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }
            }
            return ParamUtils.genScalarField(elementType, DataType.None, values);
        }
        List<Object> compacted = new ArrayList<>();
        List<Boolean> validity = new ArrayList<>();
        for (Object value : values) {
            validity.add(value != null);
            if (value != null) {
                compacted.add(value);
            }
        }
        ScalarField scalarField = ParamUtils.genScalarField(elementType, DataType.None, compacted);
        return scalarField.toBuilder().addAllValidData(validity).build();
    }

    /**
     * Encodes one struct row's vector sub-field values into a per-row VectorField cell.
     * When the sub-field is element-nullable, null vectors are compacted and the
     * element-level validity is recorded on the cell; otherwise all vectors are written
     * densely without validity.
     *
     * @param dataType the proto vector element type
     * @param vectors the row's element vectors (may contain nulls when element-nullable)
     * @param elementNullable whether the sub-field permits null vectors
     * @param dim the expected vector dimension
     * @param name the sub-field name (for error messages)
     * @return the encoded per-row VectorField cell
     */
    private static VectorField genStructVectorRow(DataType dataType, List<?> vectors,
                                                  boolean elementNullable, int dim, String name) {
        List<Boolean> validity = null;
        List<?> nonNullVectors = vectors;
        if (elementNullable) {
            List<Object> compacted = new ArrayList<>();
            validity = new ArrayList<>();
            for (Object vector : vectors) {
                validity.add(vector != null);
                if (vector != null) {
                    compacted.add(vector);
                }
            }
            nonNullVectors = compacted;
        }
        VectorField vectorField = ParamUtils.genVectorField(dataType, nonNullVectors);
        if (vectorField.getDim() == 0 && dim > 0) {
            vectorField = vectorField.toBuilder().setDim(dim).build();
        }
        if (vectorField.getDim() > 0 && dim > 0 && vectorField.getDim() != dim) {
            String msg = String.format("Dimension mismatch for field %s, expected: %d, actual: %d",
                    name, dim, vectorField.getDim());
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
        }
        if (validity != null) {
            vectorField = vectorField.toBuilder().addAllValidData(validity).build();
        }
        return vectorField;
    }

    /**
     * Encodes one struct row's nested-Array sub-field values into a per-row ScalarField
     * cell. The cell is itself an ArrayArray whose element type is the leaf element type.
     * When the sub-field is element-nullable (inner array can be null), null inner arrays
     * are compacted and the element-level validity is recorded on the cell; when the leaf
     * is element-nullable, null leaf values inside each inner array are compacted with
     * leaf-level validity.
     *
     * @param fieldSchema the nested-Array sub-field schema
     * @param innerArrays the row's inner-array values (may contain nulls when element-nullable)
     * @param name the sub-field name (for error messages)
     * @return the encoded per-row ScalarField cell
     */
    private static ScalarField genStructNestedArrayRow(CreateCollectionReq.FieldSchema fieldSchema,
                                                       List<?> innerArrays, String name) {
        boolean innerArrayNullable = Boolean.TRUE.equals(fieldSchema.getIsNullable());
        boolean leafNullable = Boolean.TRUE.equals(fieldSchema.getElementNullable());
        DataType leafType = ConvertUtils.toProtoDataType(fieldSchema.getElementType());
        ArrayArray.Builder innerBuilder = ArrayArray.newBuilder().setElementType(leafType);
        List<Boolean> validity = new ArrayList<>();
        List<Object> compacted = new ArrayList<>();
        for (Object inner : innerArrays) {
            validity.add(inner != null);
            if (inner != null) {
                compacted.add(inner);
            }
        }
        if (!innerArrayNullable && validity.contains(Boolean.FALSE)) {
            String msg = String.format("Array sub-field '%s' does not allow null inner arrays", name);
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
        }
        for (Object inner : compacted) {
            List<?> leafValues = (List<?>) inner;
            innerBuilder.addData(genStructScalarRow(leafType, leafValues, leafNullable, name));
        }
        ScalarField.Builder rowBuilder = ScalarField.newBuilder().setArrayData(innerBuilder);
        if (innerArrayNullable) {
            rowBuilder.addAllValidData(validity);
        }
        return rowBuilder.build();
    }

    /**
     * Generates a gRPC vector array for struct field data.
     *
     * @param dataType the vector data type
     * @param objects the vector values to encode
     * @param dim the expected vector dimension
     * @return the built gRPC vector array
     * @throws MilvusClientException if the data type is unsupported or the dimension mismatches
     */
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

    /**
     * Converts the given V2 delete request into a gRPC delete request.
     *
     * @param request the V2 delete request
     * @return the built gRPC delete request
     */


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
        if (request.getConsistencyLevel() != null) {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }
        if (StringUtils.isNotEmpty(request.getRlsPrincipal())) {
            builder.setRlsPrincipal(request.getRlsPrincipal());
        }
        if (request.getSkipRls() != null) {
            builder.setSkipRls(request.getSkipRls());
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

    /**
     * Validates and converts a field value against the field schema.
     *
     * @param field the field schema
     * @param fieldData the field value to check
     * @return the validated and converted field value
     */


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
