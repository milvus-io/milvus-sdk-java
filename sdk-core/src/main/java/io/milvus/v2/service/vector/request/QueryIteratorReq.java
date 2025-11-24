package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.v2.common.ConsistencyLevel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private String timezone;
    private long batchSize;
    private boolean reduceStopForBest;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

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
        this.timezone = builder.timezone;
        this.batchSize = builder.batchSize;
        this.reduceStopForBest = builder.reduceStopForBest;
        this.filterTemplateValues = builder.filterTemplateValues;
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

    public String getTimezone() {
        return timezone;
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

    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
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
                ", timezone='" + timezone + '\'' +
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
        private String timezone = "";
        private long batchSize = 1000L;
        private boolean reduceStopForBest = false;
        private Map<String, Object> filterTemplateValues = new HashMap<>();

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

        public QueryIteratorReqBuilder timezone(String timezone) {
            this.timezone = timezone;
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

        public QueryIteratorReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        public QueryIteratorReq build() {
            return new QueryIteratorReq(this);
        }
    }
}
