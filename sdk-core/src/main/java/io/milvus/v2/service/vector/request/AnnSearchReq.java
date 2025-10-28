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

import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class AnnSearchReq {
    private String vectorFieldName;
    @Deprecated
    private int topK; // deprecated, replaced by limit
    private long limit;
    @Deprecated
    private String expr; // deprecated, replaced by filter
    private String filter;
    private List<BaseVector> vectors;
    private String params;
    private IndexParam.MetricType metricType;

    private AnnSearchReq(Builder builder) {
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.expr = builder.expr;
        this.filter = builder.filter;
        this.vectors = builder.vectors;
        this.params = builder.params;
        this.metricType = builder.metricType;
    }

    public static Builder builder() {
        return new Builder();
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

    @Deprecated
    public String getExpr() {
        return expr;
    }

    @Deprecated
    public void setExpr(String expr) {
        this.expr = expr;
        this.filter = expr;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
        this.expr = filter;
    }

    public List<BaseVector> getVectors() {
        return vectors;
    }

    public void setVectors(List<BaseVector> vectors) {
        this.vectors = vectors;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    public void setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AnnSearchReq that = (AnnSearchReq) obj;
        return new EqualsBuilder()
                .append(topK, that.topK)
                .append(limit, that.limit)
                .append(vectorFieldName, that.vectorFieldName)
                .append(expr, that.expr)
                .append(filter, that.filter)
                .append(vectors, that.vectors)
                .append(params, that.params)
                .append(metricType, that.metricType)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(vectorFieldName)
                .append(topK)
                .append(limit)
                .append(expr)
                .append(filter)
                .append(vectors)
                .append(params)
                .append(metricType)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "AnnSearchReq{" +
                "vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", limit=" + limit +
                ", expr='" + expr + '\'' +
                ", filter='" + filter + '\'' +
                ", vectors=" + vectors +
                ", params='" + params + '\'' +
                ", metricType=" + metricType +
                '}';
    }

    public static class Builder {
        private String vectorFieldName;
        private int topK = 0;
        private long limit = 0L;
        private String expr = "";
        private String filter = "";
        private List<BaseVector> vectors;
        private String params;
        private IndexParam.MetricType metricType = null;

        public Builder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        // topK is deprecated replaced by limit, topK and limit must be the same value
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

        // expr is deprecated replaced by filter, expr and filter must be the same value
        @Deprecated
        public Builder expr(String val) {
            this.expr = val;
            this.filter = val;
            return this;
        }

        public Builder filter(String val) {
            this.expr = val;
            this.filter = val;
            return this;
        }

        public Builder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        public Builder params(String params) {
            this.params = params;
            return this;
        }

        public Builder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public AnnSearchReq build() {
            return new AnnSearchReq(this);
        }
    }
}
