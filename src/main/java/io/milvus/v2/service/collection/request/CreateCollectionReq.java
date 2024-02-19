package io.milvus.v2.service.collection.request;

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class CreateCollectionReq {
    private String collectionName;
    private Integer dimension;

    @Builder.Default
    private String primaryFieldName = "id";
    @Builder.Default
    private DataType primaryFieldType = DataType.Int64;
    @Builder.Default
    private Integer maxLength = 65535;
    @Builder.Default
    private String vectorFieldName = "vector";
    @Builder.Default
    private String metricType = IndexParam.MetricType.IP.name();
    @Builder.Default
    private Boolean autoID = Boolean.FALSE;
    @Builder.Default
    private Boolean enableDynamicField = Boolean.TRUE;

    // create collections with schema
    private CollectionSchema collectionSchema;

    private List<IndexParam> indexParams;

    @Data
    @SuperBuilder
    public static class CollectionSchema {
        @Builder.Default
        private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        @Builder.Default
        private String description = "";
        private Boolean enableDynamicField;

        public CreateCollectionReq.FieldSchema getField(String fieldName) {
            for (CreateCollectionReq.FieldSchema field : fieldSchemaList) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
        }

        public void addPrimaryField(String fieldName, DataType dataType, Boolean isPrimaryKey, Boolean autoID) {
            // primary key field
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name(fieldName)
                    .dataType(dataType)
                    .isPrimaryKey(isPrimaryKey)
                    .autoID(autoID)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }

        public void addPrimaryField(String fieldName, DataType dataType, Integer maxLength, Boolean isPrimaryKey, Boolean autoID) {
            // primary key field
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name(fieldName)
                    .dataType(dataType)
                    .maxLength(maxLength)
                    .isPrimaryKey(isPrimaryKey)
                    .autoID(autoID)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }

        public void addVectorField(String fieldName, DataType dataType, Integer dimension) {
            // vector field
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name(fieldName)
                    .dataType(dataType)
                    .dimension(dimension)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }

        public void addScalarField(String fieldName, DataType dataType, Integer maxLength) {
            // scalar field
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name(fieldName)
                    .dataType(dataType)
                    .maxLength(maxLength)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }

        public void addScalarField(String fieldName, DataType dataType) {
            // scalar field
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name(fieldName)
                    .dataType(dataType)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }
    }

    @Data
    @SuperBuilder
    public static class FieldSchema {
        //TODO: check here
        private String name;
        private DataType dataType;
        @Builder.Default
        private Integer maxLength = 65535;
        private Integer dimension;
        @Builder.Default
        private Boolean isPrimaryKey = Boolean.FALSE;
        @Builder.Default
        private Boolean autoID = Boolean.FALSE;
        private DataType elementType;
        private Integer maxCapacity;
    }
}
