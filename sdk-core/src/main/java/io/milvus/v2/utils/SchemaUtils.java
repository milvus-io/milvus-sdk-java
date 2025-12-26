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

import com.google.gson.reflect.TypeToken;
import io.milvus.common.utils.JsonUtils;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.milvus.param.Constant.*;
import static io.milvus.param.ParamUtils.AssembleKvPair;

public class SchemaUtils {
    protected static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);


    public static void checkNullEmptyString(String target, String title) {
        if (target == null || StringUtils.isBlank(target)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, title + " cannot be null or empty");
        }
    }

    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema) {
        return convertToGrpcFieldSchema(fieldSchema, false);
    }

    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema, boolean forAddField) {
        checkNullEmptyString(fieldSchema.getName(), "Field name");

        DataType dType = DataType.valueOf(fieldSchema.getDataType().name());

        // Vector field must be nullable when adding to existing collection
        if (forAddField && ParamUtils.isVectorDataType(dType) && !fieldSchema.getIsNullable()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Vector field must be nullable when adding to existing collection, field name: " + fieldSchema.getName());
        }
        FieldSchema.Builder builder = FieldSchema.newBuilder()
                .setName(fieldSchema.getName())
                .setDescription(fieldSchema.getDescription())
                .setDataType(dType)
                .setIsPrimaryKey(fieldSchema.getIsPrimaryKey())
                .setIsPartitionKey(fieldSchema.getIsPartitionKey())
                .setIsClusteringKey(fieldSchema.getIsClusteringKey())
                .setAutoID(fieldSchema.getAutoID())
                .setNullable(fieldSchema.getIsNullable());
        if (!ParamUtils.isVectorDataType(dType) && !fieldSchema.getIsPrimaryKey()) {
            ValueField value = ParamUtils.objectToValueField(fieldSchema.getDefaultValue(), dType);
            if (value != null) {
                builder.setDefaultValue(value);
            } else if (fieldSchema.getDefaultValue() != null) {
                String msg = String.format("Illegal default value for %s type field. Please use Short for Int8/Int16 fields, " +
                        "Short/Integer for Int32 fields, Short/Integer/Long for Int64 fields, Boolean for Bool fields, " +
                        "String for Varchar fields, JsonObject for JSON fields.", dType.name());
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
            }
        }

        // assemble typeParams for FieldSchema
        Map<String, String> typeParams = fieldSchema.getTypeParams() == null ? new HashMap<>() : fieldSchema.getTypeParams();
        if (fieldSchema.getDimension() != null) {
            typeParams.put("dim", String.valueOf(fieldSchema.getDimension()));
        }
//        if (Objects.equals(fieldSchema.getName(), partitionKeyField)) {
//            schema = schema.toBuilder().setIsPartitionKey(Boolean.TRUE).build();
//        }
        if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
            typeParams.put("max_length", String.valueOf(fieldSchema.getMaxLength()));
        }
        if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.Array) {
            builder.setElementType(DataType.valueOf(fieldSchema.getElementType().name())).build();
            if (fieldSchema.getMaxCapacity() != null) {
                typeParams.put("max_capacity", String.valueOf(fieldSchema.getMaxCapacity()));
            }

            if (fieldSchema.getElementType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
                typeParams.put("max_length", String.valueOf(fieldSchema.getMaxLength()));
            }
        }

        if (fieldSchema.getEnableAnalyzer() != null) {
            typeParams.put("enable_analyzer", String.valueOf(fieldSchema.getEnableAnalyzer()));
        }
        if (fieldSchema.getEnableMatch() != null) {
            typeParams.put("enable_match", String.valueOf(fieldSchema.getEnableMatch()));
        }
        if (fieldSchema.getAnalyzerParams() != null) {
            String params = JsonUtils.toJson(fieldSchema.getAnalyzerParams());
            typeParams.put("analyzer_params", params);
        }
        if (fieldSchema.getMultiAnalyzerParams() != null) {
            String params = JsonUtils.toJson(fieldSchema.getMultiAnalyzerParams());
            typeParams.put("multi_analyzer_params", params);
        }

        List<KeyValuePair> typeParamsList = AssembleKvPair(typeParams);
        if (CollectionUtils.isNotEmpty(typeParamsList)) {
            typeParamsList.forEach(builder::addTypeParams);
        }
        return builder.build();
    }

    public static FunctionSchema convertToGrpcFunction(CreateCollectionReq.Function function) {
        checkNullEmptyString(function.getName(), "Function name");

        FunctionSchema.Builder builder = FunctionSchema.newBuilder()
                .setName(function.getName())
                .setDescription(function.getDescription())
                .setType(FunctionType.forNumber(function.getFunctionType().getCode()));

        for (String name : function.getInputFieldNames()) {
            builder.addInputFieldNames(name);
        }
        for (String name : function.getOutputFieldNames()) {
            builder.addOutputFieldNames(name);
        }

        List<KeyValuePair> params = ParamUtils.AssembleKvPair(function.getParams());
        if (CollectionUtils.isNotEmpty(params)) {
            params.forEach(builder::addParams);
        }

        return builder.build();
    }

    public static StructArrayFieldSchema convertToGrpcStructFieldSchema(CreateCollectionReq.StructFieldSchema structSchema) {
        checkNullEmptyString(structSchema.getName(), "Field name");
        StructArrayFieldSchema.Builder builder = StructArrayFieldSchema.newBuilder()
                .setName(structSchema.getName())
                .setDescription(structSchema.getDescription());

        for (CreateCollectionReq.FieldSchema field : structSchema.getFields()) {
            DataType actualType = DataType.Array;
            DataType elementType = DataType.valueOf(field.getDataType().name());
            if (ParamUtils.isVectorDataType(elementType)) {
                actualType = DataType.ArrayOfVector;
            }
            FieldSchema fieldSchema = convertToGrpcFieldSchema(field);
            // reset data type and capacity
            fieldSchema = fieldSchema.toBuilder()
                    .setDataType(actualType)
                    .setElementType(elementType)
                    .addTypeParams(KeyValuePair.newBuilder()
                            .setKey(ARRAY_MAX_CAPACITY)
                            .setValue(String.valueOf(structSchema.getMaxCapacity()))
                            .build())
                    .build();
            builder.addFields(fieldSchema);
        }
        return builder.build();
    }

    public static CreateCollectionReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(schema.getEnableDynamicField())
                .build();

        // normal fields
        List<CreateCollectionReq.FieldSchema> fieldSchemas = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            fieldSchemas.add(convertFromGrpcFieldSchema(fieldSchema));
        }
        collectionSchema.setFieldSchemaList(fieldSchemas);

        // struct fields
        List<CreateCollectionReq.StructFieldSchema> structSchemas = new ArrayList<>();
        for (StructArrayFieldSchema fieldSchema : schema.getStructArrayFieldsList()) {
            structSchemas.add(convertFromGrpcStructFieldSchema(fieldSchema));
        }
        collectionSchema.setStructFields(structSchemas);

        // functions
        List<CreateCollectionReq.Function> functions = new ArrayList<>();
        for (FunctionSchema functionSchema : schema.getFunctionsList()) {
            functions.add(convertFromGrpcFunction(functionSchema));
        }
        collectionSchema.setFunctionList(functions);

        return collectionSchema;
    }

    public static CreateCollectionReq.FieldSchema convertFromGrpcFieldSchema(FieldSchema fieldSchema) {
        // if the fieldSchema belongs to a struct field, its type could be ArrayOfVector/ArrayOfStruct
        // in fact, its actual type is elementType
        DataType dataType = fieldSchema.getDataType();
        DataType elementType = fieldSchema.getElementType();
        io.milvus.v2.common.DataType actualType;
        io.milvus.v2.common.DataType actualElementType = io.milvus.v2.common.DataType.None;
        if (dataType == DataType.ArrayOfVector || dataType == DataType.ArrayOfStruct) {
            actualType = io.milvus.v2.common.DataType.valueOf(elementType.name());
        } else {
            actualType = io.milvus.v2.common.DataType.valueOf(dataType.name());
            actualElementType = io.milvus.v2.common.DataType.valueOf(elementType.name());
        }
        CreateCollectionReq.FieldSchema schema = CreateCollectionReq.FieldSchema.builder()
                .name(fieldSchema.getName())
                .description(fieldSchema.getDescription())
                .dataType(actualType)
                .isPrimaryKey(fieldSchema.getIsPrimaryKey())
                .isPartitionKey(fieldSchema.getIsPartitionKey())
                .isClusteringKey(fieldSchema.getIsClusteringKey())
                .autoID(fieldSchema.getAutoID())
                .elementType(actualElementType)
                .isNullable(fieldSchema.getNullable())
                .defaultValue(ParamUtils.valueFieldToObject(fieldSchema.getDefaultValue(), fieldSchema.getDataType()))
                .build();

        Map<String, String> typeParams = new HashMap<>();
        for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
            try {
                if (keyValuePair.getKey().equals(VECTOR_DIM)) {
                    schema.setDimension(Integer.parseInt(keyValuePair.getValue()));
                } else if (keyValuePair.getKey().equals(VARCHAR_MAX_LENGTH)) {
                    schema.setMaxLength(Integer.parseInt(keyValuePair.getValue()));
                } else if (keyValuePair.getKey().equals(ARRAY_MAX_CAPACITY)) {
                    schema.setMaxCapacity(Integer.parseInt(keyValuePair.getValue()));
                } else if (keyValuePair.getKey().equals("enable_analyzer")) {
                    schema.setEnableAnalyzer(Boolean.parseBoolean(keyValuePair.getValue()));
                } else if (keyValuePair.getKey().equals("enable_match")) {
                    schema.setEnableMatch(Boolean.parseBoolean(keyValuePair.getValue()));
                } else if (keyValuePair.getKey().equals("analyzer_params")) {
                    Map<String, Object> params = JsonUtils.fromJson(keyValuePair.getValue(), new TypeToken<Map<String, Object>>() {
                    }.getType());
                    schema.setAnalyzerParams(params);
                } else if (keyValuePair.getKey().equals("multi_analyzer_params")) {
                    Map<String, Object> params = JsonUtils.fromJson(keyValuePair.getValue(), new TypeToken<Map<String, Object>>() {
                    }.getType());
                    schema.setMultiAnalyzerParams(params);
                }
            } catch (Exception e) {
                /**
                 * Currently, the kernel does not enforce validation on the input `typeParams`, so this conversion may throw an exception.
                 * To prevent normal `descCollection` from malfunctioning, we wrap it here in a `try/catch` block.
                 */
                logger.error("Failed to convert the typeParams value of {} , key:{}, value:{}", fieldSchema.getName(), keyValuePair.getKey(), keyValuePair.getValue());
            }
            // To maintain compatibility with clientV1, the typeParams here will be returned in their original format.
            typeParams.put(keyValuePair.getKey(), keyValuePair.getValue());
        }
        schema.setTypeParams(typeParams);
        return schema;
    }

    public static CreateCollectionReq.StructFieldSchema convertFromGrpcStructFieldSchema(StructArrayFieldSchema structSchema) {
        CreateCollectionReq.StructFieldSchema.StructFieldSchemaBuilder builder =
                CreateCollectionReq.StructFieldSchema.builder()
                        .name(structSchema.getName())
                        .description(structSchema.getDescription());
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        for (FieldSchema fieldSchema : structSchema.getFieldsList()) {
            CreateCollectionReq.FieldSchema field = convertFromGrpcFieldSchema(fieldSchema);
            builder.maxCapacity(field.getMaxCapacity());
            // each rpc proto struct's sub-field schema, the data type is Array or ArrayOfVector, the typeParams
            // contains a "max_capacity" value
            // reset data type to element type, remove the "max_capacity" from typeParams
            field.setDataType(ConvertUtils.toSdkDataType(fieldSchema.getElementType()));
            field.setElementType(io.milvus.v2.common.DataType.None);
            Map<String, String> params = field.getTypeParams();
            params.remove(ARRAY_MAX_CAPACITY);
            field.setTypeParams(params);
            field.setMaxCapacity(0);
            fields.add(field);
        }
        builder.fields(fields);
        return builder.build();
    }

    public static CreateCollectionReq.Function convertFromGrpcFunction(FunctionSchema functionSchema) {
        CreateCollectionReq.Function.FunctionBuilder builder = CreateCollectionReq.Function.builder()
                .name(functionSchema.getName())
                .description(functionSchema.getDescription())
                .functionType(io.milvus.common.clientenum.FunctionType.fromName(functionSchema.getType().name()))
                .inputFieldNames(functionSchema.getInputFieldNamesList().stream().collect(Collectors.toList()))
                .outputFieldNames(functionSchema.getOutputFieldNamesList().stream().collect(Collectors.toList()));
        List<KeyValuePair> pairs = functionSchema.getParamsList();
        pairs.forEach((kv) -> builder.param(kv.getKey(), kv.getValue()));
        return builder.build();
    }

    public static CreateCollectionReq.FieldSchema convertFieldReqToFieldSchema(AddFieldReq addFieldReq) {
        // check the input here to pop error messages earlier
        if (addFieldReq.isEnableDefaultValue() && addFieldReq.getDefaultValue() == null
                && addFieldReq.getIsNullable() == Boolean.FALSE) {
            String msg = String.format("Default value cannot be null for field '%s' that is defined as nullable == false.", addFieldReq.getFieldName());
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
        }

        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name(addFieldReq.getFieldName())
                .dataType(addFieldReq.getDataType())
                .description(addFieldReq.getDescription())
                .isPrimaryKey(addFieldReq.getIsPrimaryKey())
                .isPartitionKey(addFieldReq.getIsPartitionKey())
                .isClusteringKey(addFieldReq.getIsClusteringKey())
                .autoID(addFieldReq.getAutoID())
                .isNullable(addFieldReq.getIsNullable())
                .defaultValue(addFieldReq.getDefaultValue())
                .enableAnalyzer(addFieldReq.getEnableAnalyzer())
                .enableMatch(addFieldReq.getEnableMatch())
                .analyzerParams(addFieldReq.getAnalyzerParams())
                .typeParams(addFieldReq.getTypeParams())
                .multiAnalyzerParams(addFieldReq.getMultiAnalyzerParams())
                .build();
        if (addFieldReq.getDataType().equals(io.milvus.v2.common.DataType.Array)) {
            if (addFieldReq.getElementType() == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Element type, maxCapacity are required for array field");
            }
            fieldSchema.setElementType(addFieldReq.getElementType());
            fieldSchema.setMaxCapacity(addFieldReq.getMaxCapacity());
            if (addFieldReq.getElementType().equals(io.milvus.v2.common.DataType.VarChar)) {
                fieldSchema.setMaxLength(addFieldReq.getMaxLength());
            }
        } else if (addFieldReq.getDataType().equals(io.milvus.v2.common.DataType.VarChar)) {
            fieldSchema.setMaxLength(addFieldReq.getMaxLength());
        } else if (ParamUtils.isDenseVectorDataType(io.milvus.grpc.DataType.valueOf(addFieldReq.getDataType().name()))) {
            if (addFieldReq.getDimension() == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Dimension is required for vector field");
            }
            fieldSchema.setDimension(addFieldReq.getDimension());
        }

        return fieldSchema;
    }

    public static CreateCollectionReq.StructFieldSchema convertFieldReqToStructFieldSchema(AddFieldReq addFieldReq) {
        List<CreateCollectionReq.FieldSchema> fields = addFieldReq.getStructFields();
        if (fields.isEmpty()) {
            throw new ParamException("Struct field must have at least one field");
        }
        String structName = addFieldReq.getFieldName();
        if (addFieldReq.getMaxCapacity() == null) {
            String msg = String.format("maxCapacity not set for struct field: '%s'", structName);
            throw new ParamException(msg);
        }

        Set<String> uniqueNames = new HashSet<>();
        for (CreateCollectionReq.FieldSchema field : fields) {
            String fieldName = field.getName();
            uniqueNames.add(fieldName);
            if (field.getIsPrimaryKey()) {
                String msg = String.format("Field '%s' in struct '%s' cannot be primary key", fieldName, structName);
                throw new ParamException(msg);
            } else if (field.getIsPartitionKey()) {
                String msg = String.format("Field '%s' in struct '%s' cannot be partition key", fieldName, structName);
                throw new ParamException(msg);
            } else if (field.getIsClusteringKey()) {
                String msg = String.format("Field '%s' in struct '%s' cannot be clustering key", fieldName, structName);
                throw new ParamException(msg);
            } else if (field.getAutoID()) {
                String msg = String.format("Field '%s' in struct '%s' cannot be auto-id", fieldName, structName);
                throw new ParamException(msg);
            } else if (field.getIsNullable()) {
                String msg = String.format("Field '%s' in struct '%s' cannot be nullable", fieldName, structName);
                throw new ParamException(msg);
            } else if (field.getDefaultValue() != null) {
                String msg = String.format("Field '%s' in struct '%s' cannot have default value", fieldName, structName);
                throw new ParamException(msg);
            }
        }
        if (uniqueNames.size() != fields.size()) {
            String msg = String.format("Duplicate field names in struct '%s'", structName);
            throw new ParamException(msg);
        }

        return CreateCollectionReq.StructFieldSchema.builder()
                .name(addFieldReq.getFieldName())
                .description(addFieldReq.getDescription())
                .fields(fields)
                .maxCapacity(addFieldReq.getMaxCapacity())
                .build();
    }
}
