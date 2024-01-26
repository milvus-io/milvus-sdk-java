package io.milvus.v2.utils;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.v2.service.collection.request.CreateCollectionWithSchemaReq;

import java.util.ArrayList;
import java.util.List;

public class SchemaUtils {
    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionWithSchemaReq.FieldSchema fieldSchema) {
        FieldSchema schema = FieldSchema.newBuilder()
                .setName(fieldSchema.getName())
                .setDataType(DataType.valueOf(fieldSchema.getDataType().name()))
                .setIsPrimaryKey(fieldSchema.getIsPrimaryKey())
                .setAutoID(fieldSchema.getAutoID())
                .build();
        if(fieldSchema.getDimension() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(fieldSchema.getDimension())).build()).build();
        }
        if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(fieldSchema.getMaxLength())).build()).build();
        }
        return schema;
    }

    public static CreateCollectionWithSchemaReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        CreateCollectionWithSchemaReq.CollectionSchema collectionSchema = CreateCollectionWithSchemaReq.CollectionSchema.builder()
                .description(schema.getDescription())
                .enableDynamicField(schema.getEnableDynamicField())
                .build();
        List<CreateCollectionWithSchemaReq.FieldSchema> fieldSchemas = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            fieldSchemas.add(convertFromGrpcFieldSchema(fieldSchema));
        }
        collectionSchema.setFieldSchemaList(fieldSchemas);
        return collectionSchema;
    }

    private static CreateCollectionWithSchemaReq.FieldSchema convertFromGrpcFieldSchema(FieldSchema fieldSchema) {
        CreateCollectionWithSchemaReq.FieldSchema schema = CreateCollectionWithSchemaReq.FieldSchema.builder()
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
