package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.v2.common.ConsistencyLevel;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class QueryIteratorReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private List<String> outputFields;
    private String expr;
    private ConsistencyLevel consistencyLevel;
    private long offset;
    private long limit;
    private boolean ignoreGrowing;
    private long batchSize;
    private boolean reduceStopForBest;

    private QueryIteratorReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.outputFields = builder.outputFields;
        this.expr = builder.expr;
        this.consistencyLevel = builder.consistencyLevel;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.batchSize = builder.batchSize;
        this.reduceStopForBest = builder.reduceStopForBest;
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

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public boolean isIgnoreGrowing() {
        return ignoreGrowing;
    }

    public void setIgnoreGrowing(boolean ignoreGrowing) {
        this.ignoreGrowing = ignoreGrowing;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isReduceStopForBest() {
        return reduceStopForBest;
    }

    public void setReduceStopForBest(boolean reduceStopForBest) {
        this.reduceStopForBest = reduceStopForBest;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        QueryIteratorReq that = (QueryIteratorReq) obj;
        return new EqualsBuilder()
                .append(offset, that.offset)
                .append(limit, that.limit)
                .append(ignoreGrowing, that.ignoreGrowing)
                .append(batchSize, that.batchSize)
                .append(reduceStopForBest, that.reduceStopForBest)
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .append(outputFields, that.outputFields)
                .append(expr, that.expr)
                .append(consistencyLevel, that.consistencyLevel)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionName)
                .append(partitionNames)
                .append(outputFields)
                .append(expr)
                .append(consistencyLevel)
                .append(offset)
                .append(limit)
                .append(ignoreGrowing)
                .append(batchSize)
                .append(reduceStopForBest)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "QueryIteratorReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", outputFields=" + outputFields +
                ", expr='" + expr + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", offset=" + offset +
                ", limit=" + limit +
                ", ignoreGrowing=" + ignoreGrowing +
                ", batchSize=" + batchSize +
                ", reduceStopForBest=" + reduceStopForBest +
                '}';
    }

    public static class Builder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private List<String> outputFields = Lists.newArrayList();
        private String expr = "";
        private ConsistencyLevel consistencyLevel = null;
        private long offset = 0;
        private long limit = -1;
        private boolean ignoreGrowing = false;
        private long batchSize = 1000L;
        private boolean reduceStopForBest = false;

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

        public Builder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public Builder expr(String expr) {
            this.expr = expr;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        public Builder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder reduceStopForBest(boolean reduceStopForBest) {
            this.reduceStopForBest = reduceStopForBest;
            return this;
        }

        public QueryIteratorReq build() {
            return new QueryIteratorReq(this);
        }
    }
}
