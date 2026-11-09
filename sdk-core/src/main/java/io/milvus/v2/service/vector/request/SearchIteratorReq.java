package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;

import java.util.List;

public class SearchIteratorReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private IndexParam.MetricType metricType;
    private String vectorFieldName;
    @Deprecated
    private int topK;
    private long limit;
    private String expr;
    private List<String> outputFields;
    private List<BaseVector> vectors;
    private int roundDecimal;
    private String params;
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String groupByFieldName;
    private long batchSize;
    private String rlsPrincipal;
    private Boolean skipRls;

    private SearchIteratorReq(SearchIteratorReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.expr = builder.expr;
        this.outputFields = builder.outputFields;
        this.vectors = builder.vectors;
        this.roundDecimal = builder.roundDecimal;
        this.params = builder.params;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.groupByFieldName = builder.groupByFieldName;
        this.batchSize = builder.batchSize;
        this.rlsPrincipal = builder.rlsPrincipal;
        this.skipRls = builder.skipRls;
    }

    public static SearchIteratorReqBuilder builder() {
        return new SearchIteratorReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    public void setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public void setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
    }

    @Deprecated
    public int getTopK() {
        return topK;
    }

    @Deprecated
    public void setTopK(int topK) {
        this.topK = topK;
        this.limit = topK;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
        this.topK = (int) limit;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    public List<BaseVector> getVectors() {
        return vectors;
    }

    public void setVectors(List<BaseVector> vectors) {
        this.vectors = vectors;
    }

    public int getRoundDecimal() {
        return roundDecimal;
    }

    public void setRoundDecimal(int roundDecimal) {
        this.roundDecimal = roundDecimal;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public boolean isIgnoreGrowing() {
        return ignoreGrowing;
    }

    public void setIgnoreGrowing(boolean ignoreGrowing) {
        this.ignoreGrowing = ignoreGrowing;
    }

    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    public void setGroupByFieldName(String groupByFieldName) {
        this.groupByFieldName = groupByFieldName;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Returns the row-level security (RLS) principal for the search.
     *
     * @return the RLS principal
     */


    public String getRlsPrincipal() {
        return rlsPrincipal;
    }

    /**
     * Sets the row-level security (RLS) principal for the search.
     *
     * @param rlsPrincipal the RLS principal
     */


    public void setRlsPrincipal(String rlsPrincipal) {
        this.rlsPrincipal = rlsPrincipal;
    }

    /**
     * Returns whether the search should skip row-level security (RLS) enforcement.
     *
     * @return {@code true} to skip RLS, {@code null} for the server default
     */


    public Boolean getSkipRls() {
        return skipRls;
    }

    /**
     * Sets whether the search should skip row-level security (RLS) enforcement.
     *
     * @param skipRls {@code true} to skip RLS, {@code null} for the server default
     */


    public void setSkipRls(Boolean skipRls) {
        this.skipRls = skipRls;
    }

    @Override
    public String toString() {
        return "SearchIteratorReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", metricType=" + metricType +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", limit=" + limit +
                ", expr='" + expr + '\'' +
                ", outputFields=" + outputFields +
                ", vectors=" + vectors +
                ", roundDecimal=" + roundDecimal +
                ", params='" + params + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", batchSize=" + batchSize +
                ", rlsPrincipal='" + rlsPrincipal + '\'' +
                ", skipRls=" + skipRls +
                '}';
    }

    public static class SearchIteratorReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
        private String vectorFieldName;
        private int topK = Constant.UNLIMITED;
        private long limit = Constant.UNLIMITED_L;
        private String expr = "";
        private List<String> outputFields = Lists.newArrayList();
        private List<BaseVector> vectors = Lists.newArrayList();
        private int roundDecimal = -1;
        private String params = "{}";
        private ConsistencyLevel consistencyLevel = null;
        private boolean ignoreGrowing = false;
        private String groupByFieldName = "";
        private long batchSize = 1000L;

        private String rlsPrincipal;
        private Boolean skipRls;

        public SearchIteratorReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public SearchIteratorReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public SearchIteratorReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public SearchIteratorReqBuilder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public SearchIteratorReqBuilder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public SearchIteratorReqBuilder topK(int val) {
            this.topK = val;
            this.limit = val;
            return this;
        }

        public SearchIteratorReqBuilder limit(long val) {
            this.topK = (int) val;
            this.limit = val;
            return this;
        }

        public SearchIteratorReqBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        public SearchIteratorReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public SearchIteratorReqBuilder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        public SearchIteratorReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        public SearchIteratorReqBuilder params(String params) {
            this.params = params;
            return this;
        }

        public SearchIteratorReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public SearchIteratorReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        public SearchIteratorReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        public SearchIteratorReqBuilder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the row-level security (RLS) principal for the search.
         *
         * @param rlsPrincipal the RLS principal
         * @return this builder
         */


        public SearchIteratorReqBuilder rlsPrincipal(String rlsPrincipal) {
            this.rlsPrincipal = rlsPrincipal;
            return this;
        }

        /**
         * Sets whether the search should skip row-level security (RLS) enforcement.
         *
         * @param skipRls {@code true} to skip RLS, {@code null} for the server default
         * @return this builder
         */


        public SearchIteratorReqBuilder skipRls(Boolean skipRls) {
            this.skipRls = skipRls;
            return this;
        }

        public SearchIteratorReq build() {
            return new SearchIteratorReq(this);
        }
    }
}
