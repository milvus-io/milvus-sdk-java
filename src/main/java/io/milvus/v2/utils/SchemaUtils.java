package io.milvus.v2.utils;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SchemaUtils {
    public static FieldSchema convertToGrpcFieldSchema(String partitionKeyField, CreateCollectionReq.FieldSchema fieldSchema) {
        FieldSchema schema = FieldSchema.newBuilder()
                .setName(fieldSchema.getName())
                .setDataType(DataType.valueOf(fieldSchema.getDataType().name()))
                .setIsPrimaryKey(fieldSchema.getIsPrimaryKey())
                .setAutoID(fieldSchema.getAutoID())
                .build();
        if(fieldSchema.getDimension() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(fieldSchema.getDimension())).build()).build();
        }
        if (Objects.equals(fieldSchema.getName(), partitionKeyField)) {
            schema = schema.toBuilder().setIsPrimaryKey(Boolean.TRUE).build();
        }
        if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(fieldSchema.getMaxLength())).build()).build();
        }
        if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.Array) {
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_capacity").setValue(String.valueOf(fieldSchema.getMaxCapacity())).build()).build();
            schema = schema.toBuilder().setElementType(DataType.valueOf(fieldSchema.getElementType().name())).build();
            if (fieldSchema.getElementType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
                schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(fieldSchema.getMaxLength())).build()).build();
            }
        }
        return schema;
    }

    public static CreateCollectionReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .description(schema.getDescription())
                .enableDynamicField(schema.getEnableDynamicField())
                .build();
        List<CreateCollectionReq.FieldSchema> fieldSchemas = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            fieldSchemas.add(convertFromGrpcFieldSchema(fieldSchema));
        }
        collectionSchema.setFieldSchemaList(fieldSchemas);
        return collectionSchema;
    }

    private static CreateCollectionReq.FieldSchema convertFromGrpcFieldSchema(FieldSchema fieldSchema) {
        CreateCollectionReq.FieldSchema schema = CreateCollectionReq.FieldSchema.builder()
                .name(fieldSchema.getName())
                .dataType(io.milvus.v2.common.DataType.valueOf(fieldSchema.getDataType().name()))
                .isPrimaryKey(fieldSchema.getIsPrimaryKey())
                .autoID(fieldSchema.getAutoID())
                .build();
        for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
            if(keyValuePair.getKey().equals("dim")){
                schema.setDimension(Integer.parseInt(keyValuePair.getValue()));
            }
        }
        return schema;
    }
}
