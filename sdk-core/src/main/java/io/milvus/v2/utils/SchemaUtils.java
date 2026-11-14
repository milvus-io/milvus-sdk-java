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

import com.google.gson.JsonObject;
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

/**
 * Helper for converting between V2 schema request/response DTOs and the gRPC schema
 * representations, and for validating field and schema inputs.
 */


public class SchemaUtils {
    protected static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);

    // a struct Array sub-field can only carry a scalar leaf element type (aligned with
    // pymilvus StructFieldSchema._check_fields)
    private static final Set<io.milvus.v2.common.DataType> ARRAY_SCALAR_ELEMENT_TYPES =
            new HashSet<>(Arrays.asList(io.milvus.v2.common.DataType.Bool, io.milvus.v2.common.DataType.Int8,
                    io.milvus.v2.common.DataType.Int16, io.milvus.v2.common.DataType.Int32,
                    io.milvus.v2.common.DataType.Int64, io.milvus.v2.common.DataType.Float,
                    io.milvus.v2.common.DataType.Double, io.milvus.v2.common.DataType.VarChar));

    // struct sub-field name form like "clips[note]", used by add/drop sub-field paths
    // (mirrors pymilvus Prepare.struct_field_path)
    public static final java.util.regex.Pattern STRUCT_SUB_FIELD_PATH =
            java.util.regex.Pattern.compile("([A-Za-z_][A-Za-z_0-9]*)\\[([A-Za-z_][A-Za-z_0-9]*)\\]");

    // PATH_REPLACE selector form like "[1]" or "[1][tags]", relative to the parent struct
    // field (aligned with pymilvus)
    public static final java.util.regex.Pattern STRUCT_PATH_REPLACE =
            java.util.regex.Pattern.compile("\\[(?:0|[1-9][0-9]*)\\](?:\\[([^\\[\\]\\s]+)\\])?");

    /**
     * Returns whether the given field name contains a bracket character, which would indicate
     * an intended struct sub-field path.
     *
     * @param fieldName the field name to check
     * @return {@code true} if the name contains {@code [} or {@code ]}
     */
    public static boolean containsBracket(String fieldName) {
        return fieldName != null && (fieldName.contains("[") || fieldName.contains("]"));
    }

    /**
     * Returns whether the given field name is a struct sub-field path like
     * {@code "clips[note]"}. Mirrors pymilvus {@code Prepare.struct_field_path}.
     *
     * @param fieldName the field name to check
     * @return {@code true} if the name is a valid struct sub-field path
     */
    public static boolean isStructSubFieldPath(String fieldName) {
        if (fieldName == null || (!fieldName.contains("[") && !fieldName.contains("]"))) {
            return false;
        }
        return STRUCT_SUB_FIELD_PATH.matcher(fieldName).matches();
    }

    /**
     * Validates the Array-specific constraints of a struct sub-field. Shared by the
     * {@code StructFieldSchema.addField} builder path and the
     * {@code convertFieldReqToStructFieldSchema} conversion path so the two cannot drift.
     *
     * @param dataType the sub-field data type
     * @param elementType the Array sub-field element type
     * @param maxCapacity the Array sub-field capacity
     * @param elementNullable the Array sub-field element-nullability flag
     * @param fieldName the sub-field name
     * @param structName the parent struct field name
     * @throws ParamException when a constraint is violated
     */
    public static void checkStructArraySubField(io.milvus.v2.common.DataType dataType,
                                                io.milvus.v2.common.DataType elementType,
                                                Integer maxCapacity,
                                                Boolean elementNullable,
                                                String fieldName, String structName) {
        if (dataType == io.milvus.v2.common.DataType.Array && elementType == null) {
            throw new ParamException(String.format("Element type is required for Array sub-field '%s' in struct '%s'",
                    fieldName, structName));
        }
        if (dataType == io.milvus.v2.common.DataType.Array
                && !ARRAY_SCALAR_ELEMENT_TYPES.contains(elementType)) {
            throw new ParamException(String.format("Array sub-field '%s' in struct '%s' requires a scalar element_type",
                    fieldName, structName));
        }
        if (dataType == io.milvus.v2.common.DataType.Array && maxCapacity == null) {
            throw new ParamException(String.format("maxCapacity is required for Array sub-field '%s' in struct '%s'",
                    fieldName, structName));
        }
        if (Boolean.TRUE.equals(elementNullable) && dataType != io.milvus.v2.common.DataType.Array) {
            throw new ParamException(String.format("elementNullable requires an Array sub-field, field '%s' in struct '%s'",
                    fieldName, structName));
        }
    }


    /**
     * Throws an invalid-parameter exception if the target string is null or blank.
     *
     * @param target the string to check
     * @param title the field name used in the error message
     * @throws MilvusClientException if the target string is null or blank
     */



    public static void checkNullEmptyString(String target, String title) {
        if (target == null || StringUtils.isBlank(target)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, title + " cannot be null or empty");
        }
    }

    /**
     * Converts a V2 field schema into a gRPC field schema.
     *
     * @param fieldSchema the V2 field schema
     * @return the built gRPC field schema
     */


    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema) {
        return convertToGrpcFieldSchema(fieldSchema, false);
    }

    /**
     * Converts a V2 field schema into a gRPC field schema.
     *
     * @param fieldSchema the V2 field schema
     * @param forAddField whether the conversion is for adding a field to an existing collection
     * @return the built gRPC field schema
     */


    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema, boolean forAddField) {
        checkNullEmptyString(fieldSchema.getName(), "Field name");

        DataType dType = DataType.valueOf(fieldSchema.getDataType().name());
        boolean isNullable = Boolean.TRUE.equals(fieldSchema.getIsNullable());

        // Vector field must be nullable when adding to existing collection
        if (forAddField && ParamUtils.isVectorDataType(dType) && !isNullable) {
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
                .setNullable(isNullable)
                .setExternalField(fieldSchema.getExternalField());
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

    /**
     * Converts a V2 function into a gRPC function schema.
     *
     * @param function the V2 function
     * @return the built gRPC function schema
     */


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

    /**
     * Converts a V2 struct field schema into a gRPC struct array field schema.
     *
     * @param structSchema the V2 struct field schema
     * @return the built gRPC struct array field schema
     */


    public static StructArrayFieldSchema convertToGrpcStructFieldSchema(CreateCollectionReq.StructFieldSchema structSchema) {
        checkNullEmptyString(structSchema.getName(), "Field name");
        StructArrayFieldSchema.Builder builder = StructArrayFieldSchema.newBuilder()
                .setName(structSchema.getName())
                .setDescription(structSchema.getDescription())
                .setNullable(Boolean.TRUE.equals(structSchema.getNullable()));

        List<KeyValuePair> typeParamsList = AssembleKvPair(structSchema.getTypeParams());
        if (CollectionUtils.isNotEmpty(typeParamsList)) {
            typeParamsList.forEach(builder::addTypeParams);
        }

        for (CreateCollectionReq.FieldSchema field : structSchema.getFields()) {
            builder.addFields(buildGrpcStructSubFieldSchema(field,
                    Boolean.TRUE.equals(structSchema.getNullable()), structSchema.getMaxCapacity()));
        }
        return builder.build();
    }

    /**
     * Builds the gRPC schema of a struct sub-field, mirroring pymilvus
     * {@code Prepare.get_struct_sub_field_schema}. Each sub-field describes one struct
     * element: its {@code nullable} permits a null element value (mapped to
     * {@code element_nullable} on the wire), and the struct's row-level {@code nullable}
     * is propagated to every sub-field. A nested Array sub-field additionally builds a
     * recursive {@code type_schema} that carries the inner array's own nullability
     * ({@code element_nullable} of the sub-field), the inner element type, inner
     * {@code max_capacity} and {@code max_length}.
     *
     * @param field the V2 struct sub-field schema
     * @param rowNullable the row-level nullability inherited from the struct field
     * @param outerCapacity the struct field's per-row element capacity
     * @return the built gRPC sub-field schema
     */
    public static FieldSchema buildGrpcStructSubFieldSchema(CreateCollectionReq.FieldSchema field,
                                                             boolean rowNullable, Integer outerCapacity) {
        checkNullEmptyString(field.getName(), "Field name");
        io.milvus.v2.common.DataType sdkType = field.getDataType();
        boolean elementNullable = Boolean.TRUE.equals(field.getIsNullable());
        DataType grpcElementType = DataType.valueOf(sdkType.name());
        DataType grpcDataType;
        if (ParamUtils.isVectorDataType(grpcElementType)) {
            grpcDataType = DataType.ArrayOfVector;
        } else {
            grpcDataType = DataType.Array;
        }

        FieldSchema.Builder builder = FieldSchema.newBuilder()
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setDataType(grpcDataType)
                .setElementType(grpcElementType)
                .setNullable(rowNullable)
                .setElementNullable(elementNullable)
                .setIsPrimaryKey(false)
                .setIsPartitionKey(false)
                .setIsClusteringKey(false)
                .setAutoID(false)
                .setExternalField(field.getExternalField());

        // element-level params (dim / max_length) come from the scalar or vector sub-field
        if (field.getDimension() != null) {
            builder.addTypeParams(KeyValuePair.newBuilder()
                    .setKey(VECTOR_DIM)
                    .setValue(String.valueOf(field.getDimension()))
                    .build());
        }
        if (sdkType == io.milvus.v2.common.DataType.VarChar && field.getMaxLength() != null) {
            builder.addTypeParams(KeyValuePair.newBuilder()
                    .setKey(VARCHAR_MAX_LENGTH)
                    .setValue(String.valueOf(field.getMaxLength()))
                    .build());
        }

        // preserve any typeParams configured directly on the sub-field (e.g. dim via
        // typeParams); skip keys already emitted above and the outer capacity
        if (field.getTypeParams() != null) {
            for (Map.Entry<String, String> entry : field.getTypeParams().entrySet()) {
                String key = entry.getKey();
                if (VECTOR_DIM.equals(key) || VARCHAR_MAX_LENGTH.equals(key) || ARRAY_MAX_CAPACITY.equals(key)) {
                    continue;
                }
                builder.addTypeParams(KeyValuePair.newBuilder()
                        .setKey(key)
                        .setValue(entry.getValue())
                        .build());
            }
        }

        if (sdkType == io.milvus.v2.common.DataType.Array) {
            // nested Array sub-field: element type is ARRAY and the recursive type_schema
            // describes the inner array and its leaf values
            if (field.getElementType() == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Element type is required for Array sub-field: " + field.getName());
            }
            if (field.getMaxCapacity() == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "maxCapacity is required for Array sub-field: " + field.getName());
            }
            builder.setElementType(DataType.Array);
            TypeSchema.Builder leaf = TypeSchema.newBuilder()
                    .setLeafType(DataType.valueOf(field.getElementType().name()))
                    .setNullable(Boolean.TRUE.equals(field.getElementNullable()));
            if (field.getElementType() == io.milvus.v2.common.DataType.VarChar && field.getMaxLength() != null) {
                leaf.addTypeParams(KeyValuePair.newBuilder()
                        .setKey(VARCHAR_MAX_LENGTH)
                        .setValue(String.valueOf(field.getMaxLength()))
                        .build());
            }
            TypeSchema.Builder inner = TypeSchema.newBuilder()
                    .setNullable(elementNullable)
                    .setArrayElement(leaf.build());
            inner.addTypeParams(KeyValuePair.newBuilder()
                    .setKey(ARRAY_MAX_CAPACITY)
                    .setValue(String.valueOf(field.getMaxCapacity()))
                    .build());
            TypeSchema.Builder root = TypeSchema.newBuilder()
                    .setNullable(rowNullable)
                    .setArrayElement(inner.build());
            if (outerCapacity != null) {
                root.addTypeParams(KeyValuePair.newBuilder()
                        .setKey(ARRAY_MAX_CAPACITY)
                        .setValue(String.valueOf(outerCapacity))
                        .build());
            }
            builder.setTypeSchema(root.build());
        }

        if (outerCapacity != null) {
            builder.addTypeParams(KeyValuePair.newBuilder()
                    .setKey(ARRAY_MAX_CAPACITY)
                    .setValue(String.valueOf(outerCapacity))
                    .build());
        }
        return builder.build();
    }

    /**
     * Converts a gRPC collection schema into a V2 collection schema.
     *
     * @param schema the gRPC collection schema
     * @return the converted V2 collection schema
     */


    public static CreateCollectionReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        JsonObject externalSpec;
        try {
            externalSpec = JsonUtils.parseFromString(schema.getExternalSpec());
        } catch (Exception e) {
            logger.warn("Failed to parse externalSpec: {}", schema.getExternalSpec(), e);
            externalSpec = new JsonObject();
        }
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(schema.getEnableDynamicField())
                .externalSource(schema.getExternalSource())
                .externalSpec(externalSpec)
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

    /**
     * Converts a gRPC field schema into a V2 field schema.
     *
     * @param fieldSchema the gRPC field schema
     * @return the converted V2 field schema
     */


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
                .elementNullable(fieldSchema.getElementNullable())
                .defaultValue(ParamUtils.valueFieldToObject(fieldSchema.getDefaultValue(), fieldSchema.getDataType()))
                .externalField(fieldSchema.getExternalField())
                .build();
        // server-assigned/derived attributes, populated only by describe_collection
        schema.setFieldId(fieldSchema.getFieldID());
        schema.setIsDynamic(fieldSchema.getIsDynamic());
        schema.setIsFunctionOutput(fieldSchema.getIsFunctionOutput());

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

        Map<String, Object> indexParams = new HashMap<>();
        for (KeyValuePair keyValuePair : fieldSchema.getIndexParamsList()) {
            if (io.milvus.param.Constant.PARAMS.equals(keyValuePair.getKey())) {
                try {
                    indexParams.put(keyValuePair.getKey(), JsonUtils.fromJson(keyValuePair.getValue(),
                            new TypeToken<Map<String, Object>>() {
                            }.getType()));
                } catch (Exception e) {
                    /**
                     * The kernel does not enforce validation on the input index `params`, so this conversion may throw an exception.
                     * To prevent normal `descCollection` from malfunctioning, we wrap it here in a `try/catch` block.
                     */
                    logger.error("Failed to convert the index params value of {} , key:{}, value:{}", fieldSchema.getName(), keyValuePair.getKey(), keyValuePair.getValue());
                }
            } else {
                indexParams.put(keyValuePair.getKey(), keyValuePair.getValue());
            }
        }
        if (!indexParams.isEmpty()) {
            schema.setIndexes(Collections.singletonList(indexParams));
        } else {
            schema.setIndexes(new ArrayList<>());
        }
        return schema;
    }

    /**
     * Converts a gRPC struct array field schema into a V2 struct field schema.
     *
     * @param structSchema the gRPC struct array field schema
     * @return the converted V2 struct field schema
     */


    public static CreateCollectionReq.StructFieldSchema convertFromGrpcStructFieldSchema(StructArrayFieldSchema structSchema) {
        CreateCollectionReq.StructFieldSchema.StructFieldSchemaBuilder builder =
                CreateCollectionReq.StructFieldSchema.builder()
                        .name(structSchema.getName())
                        .description(structSchema.getDescription())
                        .nullable(structSchema.getNullable());
        List<KeyValuePair> structTypeParams = structSchema.getTypeParamsList();
        if (CollectionUtils.isNotEmpty(structTypeParams)) {
            structTypeParams.forEach((kv) -> builder.typeParam(kv.getKey(), kv.getValue()));
        }
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        for (FieldSchema fieldSchema : structSchema.getFieldsList()) {
            CreateCollectionReq.FieldSchema field = convertFromGrpcFieldSchema(fieldSchema);
            // a sub-field added via add_collection_field keeps the wire name "struct[sub]";
            // strip the struct prefix so the sub-field is addressed by its plain name
            // (aligned with pymilvus _strip_struct_sub_field_name) on describe and in DML
            // key matching
            field.setName(stripStructSubFieldName(structSchema.getName(), field.getName()));
            builder.maxCapacity(field.getMaxCapacity());
            // each rpc proto struct's sub-field schema, the data type is Array or ArrayOfVector, the typeParams
            // contains a "max_capacity" value (the outer capacity inherited from the struct)
            // element_nullable on the wire is the element-level nullability of the sub-field;
            // wire nullable (row-level, inherited from the struct) is mapped to the struct itself
            field.setIsNullable(fieldSchema.getElementNullable());
            if (fieldSchema.getElementType() == DataType.Array && fieldSchema.hasTypeSchema()) {
                // nested Array sub-field: the recursive type_schema carries the inner array
                // capacity and the leaf type/nullability; the sub-field element type is the
                // inner leaf type
                TypeSchema typeSchema = fieldSchema.getTypeSchema();
                if (typeSchema.hasArrayElement()) {
                    TypeSchema inner = typeSchema.getArrayElement();
                    if (inner.hasArrayElement()) {
                        TypeSchema leaf = inner.getArrayElement();
                        io.milvus.v2.common.DataType leafType = ConvertUtils.toSdkDataType(leaf.getLeafType());
                        field.setDataType(io.milvus.v2.common.DataType.Array);
                        field.setElementType(leafType);
                        field.setElementNullable(leaf.getNullable());
                        for (KeyValuePair kv : inner.getTypeParamsList()) {
                            if (kv.getKey().equals(ARRAY_MAX_CAPACITY)) {
                                field.setMaxCapacity(Integer.parseInt(kv.getValue()));
                            }
                        }
                        for (KeyValuePair kv : leaf.getTypeParamsList()) {
                            if (kv.getKey().equals(VARCHAR_MAX_LENGTH)) {
                                field.setMaxLength(Integer.parseInt(kv.getValue()));
                            }
                        }
                    }
                }
            } else {
                // scalar or vector sub-field: reset data type to the element type and clear
                // the inherited outer capacity
                field.setDataType(ConvertUtils.toSdkDataType(fieldSchema.getElementType()));
                field.setElementType(io.milvus.v2.common.DataType.None);
                field.setMaxCapacity(0);
            }
            Map<String, String> params = field.getTypeParams();
            params.remove(ARRAY_MAX_CAPACITY);
            field.setTypeParams(params);
            fields.add(field);
        }
        builder.fields(fields);
        return builder.build();
    }

    /**
     * Strips the "{@code structName}[" prefix and trailing "] from a struct sub-field wire
     * name, mirroring pymilvus {@code _strip_struct_sub_field_name}. A sub-field declared at
     * create time already uses its plain name and is returned unchanged.
     *
     * @param structName the parent struct field name
     * @param fieldName the wire sub-field name (may be "{@code structName}[sub]")
     * @return the plain sub-field name
     */
    public static String stripStructSubFieldName(String structName, String fieldName) {
        String prefix = structName + "[";
        if (fieldName != null && fieldName.startsWith(prefix) && fieldName.endsWith("]")) {
            return fieldName.substring(prefix.length(), fieldName.length() - 1);
        }
        return fieldName;
    }

    /**
     * Converts a gRPC function schema into a V2 function.
     *
     * @param functionSchema the gRPC function schema
     * @return the converted V2 function
     */


    public static CreateCollectionReq.Function convertFromGrpcFunction(FunctionSchema functionSchema) {
        io.milvus.common.clientenum.FunctionType functionType =
                io.milvus.common.clientenum.FunctionType.fromCode(functionSchema.getTypeValue());

        CreateCollectionReq.Function.FunctionBuilder builder = CreateCollectionReq.Function.builder()
                .name(functionSchema.getName())
                .description(functionSchema.getDescription())
                .functionType(functionType)
                .inputFieldNames(functionSchema.getInputFieldNamesList().stream().collect(Collectors.toList()))
                .outputFieldNames(functionSchema.getOutputFieldNamesList().stream().collect(Collectors.toList()));
        List<KeyValuePair> pairs = functionSchema.getParamsList();
        pairs.forEach((kv) -> builder.param(kv.getKey(), kv.getValue()));
        CreateCollectionReq.Function function = builder.build();
        // server-assigned/derived attributes, populated only by describe_collection
        function.setId(functionSchema.getId());
        function.setInputFieldIds(functionSchema.getInputFieldIdsList().stream().collect(Collectors.toList()));
        function.setOutputFieldIds(functionSchema.getOutputFieldIdsList().stream().collect(Collectors.toList()));
        return function;
    }

    /**
     * Converts an {@link AddFieldReq} into a V2 field schema, validating the field inputs.
     *
     * @param addFieldReq the add-field request
     * @return the built V2 field schema
     */


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
                .elementNullable(addFieldReq.getElementNullable())
                .defaultValue(addFieldReq.getDefaultValue())
                .enableAnalyzer(addFieldReq.getEnableAnalyzer())
                .enableMatch(addFieldReq.getEnableMatch())
                .analyzerParams(addFieldReq.getAnalyzerParams())
                .typeParams(addFieldReq.getTypeParams())
                .multiAnalyzerParams(addFieldReq.getMultiAnalyzerParams())
                .externalField(addFieldReq.getExternalField())
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

    /**
     * Converts an {@link AddFieldReq} into a V2 struct field schema, validating the struct fields.
     *
     * @param addFieldReq the add-field request
     * @return the built V2 struct field schema
     */


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
            } else if (field.getDefaultValue() != null) {
                String msg = String.format("Field '%s' in struct '%s' cannot have default value", fieldName, structName);
                throw new ParamException(msg);
            }
            if (field.getDataType() == io.milvus.v2.common.DataType.Struct) {
                String msg = String.format("Struct field schema does not support Struct as a sub-field: '%s' in struct '%s'",
                        fieldName, structName);
                throw new ParamException(msg);
            }
            checkStructArraySubField(field.getDataType(), field.getElementType(), field.getMaxCapacity(),
                    field.getElementNullable(), fieldName, structName);
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
