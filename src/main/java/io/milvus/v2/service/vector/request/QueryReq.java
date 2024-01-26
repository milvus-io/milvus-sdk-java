package io.milvus.v2.service.vector.request;

import io.milvus.v2.common.ConsistencyLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class QueryReq {
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = new ArrayList<>();
    private List<String> outputFields;
    private String expr;
    private long travelTimestamp;
    private long guaranteeTimestamp;
    private long gracefulTime;
    @Builder.Default
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
    private long offset;
    private long limit;
    private boolean ignoreGrowing;
}
