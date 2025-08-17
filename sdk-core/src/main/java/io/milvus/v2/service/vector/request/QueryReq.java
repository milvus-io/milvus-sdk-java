/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.service.vector.request;

import io.milvus.v2.common.ConsistencyLevel;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

public class QueryReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private List<String> outputFields;
    private List<Object> ids;
    private String filter;
    private ConsistencyLevel consistencyLevel;
    private long offset;
    private long limit;
    private boolean ignoreGrowing;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private QueryReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.outputFields = builder.outputFields;
        this.ids = builder.ids;
        this.filter = builder.filter;
        this.consistencyLevel = builder.consistencyLevel;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.filterTemplateValues = builder.filterTemplateValues;
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

    public List<Object> getIds() {
        return ids;
    }

    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
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

    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    public void setFilterTemplateValues(Map<String, Object> filterTemplateValues) {
        this.filterTemplateValues = filterTemplateValues;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        QueryReq that = (QueryReq) obj;
        return new EqualsBuilder()
                .append(offset, that.offset)
                .append(limit, that.limit)
                .append(ignoreGrowing, that.ignoreGrowing)
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .append(outputFields, that.outputFields)
                .append(ids, that.ids)
                .append(filter, that.filter)
                .append(consistencyLevel, that.consistencyLevel)
                .append(filterTemplateValues, that.filterTemplateValues)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionName)
                .append(partitionNames)
                .append(outputFields)
                .append(ids)
                .append(filter)
                .append(consistencyLevel)
                .append(offset)
                .append(limit)
                .append(ignoreGrowing)
                .append(filterTemplateValues)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "QueryReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", outputFields=" + outputFields +
                ", ids=" + ids +
                ", filter='" + filter + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", offset=" + offset +
                ", limit=" + limit +
                ", ignoreGrowing=" + ignoreGrowing +
                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static class Builder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();
        private List<String> outputFields = Collections.singletonList("*");
        private List<Object> ids;
        private String filter;
        private ConsistencyLevel consistencyLevel = null;
        private long offset;
        private long limit;
        private boolean ignoreGrowing;
        private Map<String, Object> filterTemplateValues = new HashMap<>();

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

        public Builder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
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

        public Builder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        public QueryReq build() {
            return new QueryReq(this);
        }
    }
}
