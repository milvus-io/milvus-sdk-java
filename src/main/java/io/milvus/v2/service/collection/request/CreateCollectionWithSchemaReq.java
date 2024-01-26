package io.milvus.v2.service.collection.request;

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@Data
public class CreateCollectionWithSchemaReq {
    private String collectionName;
    private CollectionSchema collectionSchema;
    private List<IndexParam> indexParams;

    @Data
    @SuperBuilder
    public static class CollectionSchema {
        private List<FieldSchema> fieldSchemaList;
        @Builder.Default
        private String description = "";
        private Boolean enableDynamicField;

        public FieldSchema getField(String fieldName) {
            for (FieldSchema field : fieldSchemaList) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
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
    }


}
