package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.v2.common.ConsistencyLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class QueryIteratorReq {
    private String databaseName;
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = Lists.newArrayList();
    @Builder.Default
    private List<String> outputFields = Lists.newArrayList();
    @Builder.Default
    private String expr = "";
    @Builder.Default
    private ConsistencyLevel consistencyLevel = null;
    @Builder.Default
    private long offset = 0;
    @Builder.Default
    private long limit = -1;
    @Builder.Default
    private boolean ignoreGrowing = false;
    @Builder.Default
    private long batchSize = 1000L;
    @Builder.Default
    private boolean reduceStopForBest = false;
}
