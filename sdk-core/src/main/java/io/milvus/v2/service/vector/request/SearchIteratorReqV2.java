package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SearchIteratorReqV2 {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private IndexParam.MetricType metricType;
    private String vectorFieldName;
    @Deprecated
    private int topK;
    private long limit;
    private String filter;
    private List<String> outputFields;
    private List<BaseVector> vectors;
    private int roundDecimal;
    private Map<String, Object> searchParams;
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String groupByFieldName;
    private long batchSize;
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc;

    private SearchIteratorReqV2(Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.filter = builder.filter;
        this.outputFields = builder.outputFields;
        this.vectors = builder.vectors;
        this.roundDecimal = builder.roundDecimal;
        this.searchParams = builder.searchParams;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.groupByFieldName = builder.groupByFieldName;
        this.batchSize = builder.batchSize;
        this.externalFilterFunc = builder.externalFilterFunc;
    }

    public static Builder builder() {
        return new Builder();
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
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
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

    public Map<String, Object> getSearchParams() {
        return searchParams;
    }

    public void setSearchParams(Map<String, Object> searchParams) {
        this.searchParams = searchParams;
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

    public Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> getExternalFilterFunc() {
        return externalFilterFunc;
    }

    public void setExternalFilterFunc(Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc) {
        this.externalFilterFunc = externalFilterFunc;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SearchIteratorReqV2 that = (SearchIteratorReqV2) obj;
        return new EqualsBuilder()
                .append(topK, that.topK)
                .append(limit, that.limit)
                .append(roundDecimal, that.roundDecimal)
                .append(ignoreGrowing, that.ignoreGrowing)
                .append(batchSize, that.batchSize)
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .append(metricType, that.metricType)
                .append(vectorFieldName, that.vectorFieldName)
                .append(filter, that.filter)
                .append(outputFields, that.outputFields)
                .append(vectors, that.vectors)
                .append(searchParams, that.searchParams)
                .append(consistencyLevel, that.consistencyLevel)
                .append(groupByFieldName, that.groupByFieldName)
                .append(externalFilterFunc, that.externalFilterFunc)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionName)
                .append(partitionNames)
                .append(metricType)
                .append(vectorFieldName)
                .append(topK)
                .append(limit)
                .append(filter)
                .append(outputFields)
                .append(vectors)
                .append(roundDecimal)
                .append(searchParams)
                .append(consistencyLevel)
                .append(ignoreGrowing)
                .append(groupByFieldName)
                .append(batchSize)
                .append(externalFilterFunc)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "SearchIteratorReqV2{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", metricType=" + metricType +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", limit=" + limit +
                ", filter='" + filter + '\'' +
                ", outputFields=" + outputFields +
                ", vectors=" + vectors +
                ", roundDecimal=" + roundDecimal +
                ", searchParams=" + searchParams +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", batchSize=" + batchSize +
                ", externalFilterFunc=" + externalFilterFunc +
                '}';
    }

    public static class Builder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
        private String vectorFieldName;
        private int topK = Constant.UNLIMITED;
        private long limit = Constant.UNLIMITED_L;
        private String filter = "";
        private List<String> outputFields = Lists.newArrayList();
        private List<BaseVector> vectors = Lists.newArrayList();
        private int roundDecimal = -1;
        private Map<String, Object> searchParams = new HashMap<>();
        private ConsistencyLevel consistencyLevel = null;
        private boolean ignoreGrowing = false;
        private String groupByFieldName = "";
        private long batchSize = 1000L;
        private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public Builder topK(int val) {
            this.topK = val;
            this.limit = val;
            return this;
        }

        public Builder limit(long val) {
            this.topK = (int) val;
            this.limit = val;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public Builder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        public Builder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        public Builder searchParams(Map<String, Object> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        public Builder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        public Builder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder externalFilterFunc(Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc) {
            this.externalFilterFunc = externalFilterFunc;
            return this;
        }

        public SearchIteratorReqV2 build() {
            return new SearchIteratorReqV2(this);
        }
    }
}
