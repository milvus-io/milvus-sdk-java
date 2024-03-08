package io.milvus.v2.service.vector.request;

import io.milvus.v2.common.ConsistencyLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class SearchReq {
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = new ArrayList<>();
    @Builder.Default
    private String annsField = "";
    private int topK;
    private String filter;
    @Builder.Default
    private List<String> outputFields = new ArrayList<>();
    private List<?> data;
    private long offset;
    private long limit;

    @Builder.Default
    private int roundDecimal = -1;
    @Builder.Default
    private Map<String, Object> searchParams = new HashMap<>();
    private long guaranteeTimestamp;
    @Builder.Default
    private Long gracefulTime = 5000L;
    @Builder.Default
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
    private boolean ignoreGrowing;
}
