package io.milvus.v2.service.vector.request;

import io.milvus.v2.common.ConsistencyLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@SuperBuilder
public class QueryReq {
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = new ArrayList<>();
    @Builder.Default
    private List<String> outputFields = Collections.singletonList("*");
    private List<Object> ids;
    private String filter;
    @Builder.Default
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
    private long offset;
    private long limit;
}
