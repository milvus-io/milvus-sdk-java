package io.milvus.v2.service.index.request;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeIndexReq {
    @NonNull
    private String collectionName;
    private String fieldName;
    private String indexName;
}
