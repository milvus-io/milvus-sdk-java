package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Data
@SuperBuilder
public class SearchIteratorReqV2 {
    private String databaseName;
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = Lists.newArrayList();
    @Builder.Default
    private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
    private String vectorFieldName;
    @Builder.Default
    @Deprecated
    private int topK = Constant.UNLIMITED;
    @Builder.Default
    private long limit = Constant.UNLIMITED_L;
    @Builder.Default
    private String filter = "";
    @Builder.Default
    private List<String> outputFields = Lists.newArrayList();
    @Builder.Default
    private List<BaseVector> vectors = Lists.newArrayList();
    @Builder.Default
    private int roundDecimal = -1;
    @Builder.Default
    private Map<String, Object> searchParams = new HashMap<>();
    @Builder.Default
    private ConsistencyLevel consistencyLevel = null;
    @Builder.Default
    private boolean ignoreGrowing = false;
    @Builder.Default
    private String groupByFieldName = "";
    @Builder.Default
    private long batchSize = 1000L;
    @Builder.Default
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;

    public static abstract class SearchIteratorReqV2Builder<C extends SearchIteratorReqV2, B extends SearchIteratorReqV2.SearchIteratorReqV2Builder<C, B>> {
        // topK is deprecated, topK and limit must be the same value
        public B topK(int val) {
            this.topK$value = val;
            this.topK$set = true;
            this.limit$value = val;
            this.limit$set = true;
            return self();
        }
        public B limit(long val) {
            this.topK$value = (int)val;
            this.topK$set = true;
            this.limit$value = val;
            this.limit$set = true;
            return self();
        }
    }
}
