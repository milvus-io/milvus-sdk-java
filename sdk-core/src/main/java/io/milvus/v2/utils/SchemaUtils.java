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
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.FunctionSchema;
import io.milvus.grpc.FunctionType;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ValueField;
import io.milvus.param.ParamUtils;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.milvus.param.ParamUtils.AssembleKvPair;

public class SchemaUtils {
    protected static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);


    public static void checkNullEmptyString(String target, String title) {
        if (target == null || StringUtils.isBlank(target)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, title + " cannot be null or empty");
        }
    }
    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema) {
        checkNullEmptyString(fieldSchema.getName(), "Field name");

        DataType dType = DataType.valueOf(fieldSchema.getDataType().name());
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
        if(fieldSchema.getDimension() != null){
            typeParams.put("dim", String.valueOf(fieldSchema.getDimension()));
        }
//        if (Objects.equals(fieldSchema.getName(), partitionKeyField)) {
//            schema = schema.toBuilder().setIsPartitionKey(Boolean.TRUE).build();
//        }
        if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
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
                .setType(FunctionType.valueOf(function.getFunctionType().name()));

        for (String name : function.getInputFieldNames()) {
            builder.addInputFieldNames(name);
        }
        for (String name : function.getOutputFieldNames()) {
            builder.addOutputFieldNames(name);
        }

        return builder.build();
    }

    public static CreateCollectionReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(schema.getEnableDynamicField())
                .build();
        List<CreateCollectionReq.FieldSchema> fieldSchemas = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            fieldSchemas.add(convertFromGrpcFieldSchema(fieldSchema));
        }
        collectionSchema.setFieldSchemaList(fieldSchemas);

        List<CreateCollectionReq.Function> functions = new ArrayList<>();
        for (FunctionSchema functionSchema : schema.getFunctionsList()) {
            functions.add(convertFromGrpcFunction(functionSchema));
        }
        collectionSchema.setFunctionList(functions);

        return collectionSchema;
    }

    private static CreateCollectionReq.FieldSchema convertFromGrpcFieldSchema(FieldSchema fieldSchema) {
        CreateCollectionReq.FieldSchema schema = CreateCollectionReq.FieldSchema.builder()
                .name(fieldSchema.getName())
                .description(fieldSchema.getDescription())
                .dataType(io.milvus.v2.common.DataType.valueOf(fieldSchema.getDataType().name()))
                .isPrimaryKey(fieldSchema.getIsPrimaryKey())
                .isPartitionKey(fieldSchema.getIsPartitionKey())
                .isClusteringKey(fieldSchema.getIsClusteringKey())
                .autoID(fieldSchema.getAutoID())
                .elementType(io.milvus.v2.common.DataType.valueOf(fieldSchema.getElementType().name()))
                .isNullable(fieldSchema.getNullable())
                .defaultValue(ParamUtils.valueFieldToObject(fieldSchema.getDefaultValue(), fieldSchema.getDataType()))
                .build();

        Map<String, String> typeParams = new HashMap<>();
        for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
            try {
                if(keyValuePair.getKey().equals("dim")){
                    schema.setDimension(Integer.parseInt(keyValuePair.getValue()));
                } else if(keyValuePair.getKey().equals("max_length")){
                    schema.setMaxLength(Integer.parseInt(keyValuePair.getValue()));
                } else if(keyValuePair.getKey().equals("max_capacity")){
                    schema.setMaxCapacity(Integer.parseInt(keyValuePair.getValue()));
                } else if(keyValuePair.getKey().equals("enable_analyzer")){
                    schema.setEnableAnalyzer(Boolean.parseBoolean(keyValuePair.getValue()));
                } else if(keyValuePair.getKey().equals("enable_match")){
                    schema.setEnableMatch(Boolean.parseBoolean(keyValuePair.getValue()));
                } else if(keyValuePair.getKey().equals("analyzer_params")){
                    Map<String, Object> params = JsonUtils.fromJson(keyValuePair.getValue(), new TypeToken<Map<String, Object>>() {}.getType());
                    schema.setAnalyzerParams(params);
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

    public static CreateCollectionReq.Function convertFromGrpcFunction(FunctionSchema functionSchema) {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name(functionSchema.getName())
                .description(functionSchema.getDescription())
                .functionType(io.milvus.common.clientenum.FunctionType.valueOf(functionSchema.getType().name()))
                .inputFieldNames(functionSchema.getInputFieldNamesList().stream().collect(Collectors.toList()))
                .outputFieldNames(functionSchema.getOutputFieldNamesList().stream().collect(Collectors.toList()))
                .build();
        return function;
    }
}
