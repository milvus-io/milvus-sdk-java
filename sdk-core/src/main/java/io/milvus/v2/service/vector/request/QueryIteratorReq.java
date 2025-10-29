package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.v2.common.ConsistencyLevel;

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

    private QueryIteratorReq(QueryIteratorReqBuilder builder) {
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

    public static QueryIteratorReqBuilder builder() {
        return new QueryIteratorReqBuilder();
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

    public static class QueryIteratorReqBuilder {
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

        public QueryIteratorReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public QueryIteratorReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public QueryIteratorReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public QueryIteratorReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public QueryIteratorReqBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        public QueryIteratorReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public QueryIteratorReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public QueryIteratorReqBuilder limit(long limit) {
            this.limit = limit;
            return this;
        }

        public QueryIteratorReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        public QueryIteratorReqBuilder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public QueryIteratorReqBuilder reduceStopForBest(boolean reduceStopForBest) {
            this.reduceStopForBest = reduceStopForBest;
            return this;
        }

        public QueryIteratorReq build() {
            return new QueryIteratorReq(this);
        }
    }
}
