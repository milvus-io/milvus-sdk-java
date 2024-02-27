package io.milvus.v2.service.index.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeIndexReq {
    private String collectionName;
    private String fieldName;
    private String indexName;
}
