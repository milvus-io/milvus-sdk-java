package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class SearchIteratorReq {
    private String databaseName;
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = Lists.newArrayList();
    @Builder.Default
    private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
    private String vectorFieldName;
    @Builder.Default
    private int topK = -1;
    @Builder.Default
    private String expr = "";
    @Builder.Default
    private List<String> outputFields = Lists.newArrayList();
    @Builder.Default
    private List<BaseVector> vectors = Lists.newArrayList();
    @Builder.Default
    private int roundDecimal = -1;
    @Builder.Default
    private String params = "{}";
    @Builder.Default
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
    @Builder.Default
    private boolean ignoreGrowing = false;
    @Builder.Default
    private long batchSize = 1000L;
}
