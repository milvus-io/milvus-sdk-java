package io.milvus.v2.service.collection.request;

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class CreateCollectionReq {
    private String collectionName;
    private Integer dimension;

    @Builder.Default
    private String primaryFieldName = "id";
    @Builder.Default
    private String primaryFieldType = DataType.VarChar.name();
    @Builder.Default
    private Integer maxLength = 65535;
    @Builder.Default
    private String vectorFieldName = "vector";
    @Builder.Default
    private String metricType = IndexParam.MetricType.IP.name();
    @Builder.Default
    private Boolean autoID = Boolean.TRUE;
}
