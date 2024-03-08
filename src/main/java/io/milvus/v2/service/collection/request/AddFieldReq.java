package io.milvus.v2.service.collection.request;

import io.milvus.v2.common.DataType;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class AddFieldReq {
    private String fieldName;
    @Builder.Default
    private String description = "";
    private DataType dataType;
    @Builder.Default
    private Integer maxLength = 65535;
    @Builder.Default
    private Boolean isPrimaryKey = Boolean.FALSE;
    @Builder.Default
    private Boolean isPartitionKey = Boolean.FALSE;
    @Builder.Default
    private Boolean autoID = Boolean.FALSE;
    private Integer dimension;
    private io.milvus.v2.common.DataType elementType;
    private Integer maxCapacity;
}
