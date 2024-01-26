package io.milvus.v2.service.index.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeIndexResp {
    private String indexName;
    private String indexType;
    private String metricType;
    private String fieldName;
}
