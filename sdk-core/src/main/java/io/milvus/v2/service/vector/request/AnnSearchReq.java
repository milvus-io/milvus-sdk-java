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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for an ANN (approximate nearest neighbor) search, used as part of a
 * {@link HybridSearchReq hybrid search}.
 */
public class AnnSearchReq {
    private String vectorFieldName;
    /**
     * @deprecated replaced by {@link #limit}
     */
    @Deprecated
    private int topK;
    private long limit;
    /**
     * @deprecated replaced by {@link #filter}
     */
    @Deprecated
    private String expr;
    private String filter;
    private List<BaseVector> vectors;
    private String params;
    private IndexParam.MetricType metricType;
    private String timezone;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private AnnSearchReq(AnnSearchReqBuilder builder) {
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.expr = builder.expr;
        this.filter = builder.filter;
        this.vectors = builder.vectors;
        this.params = builder.params;
        this.metricType = builder.metricType;
        this.timezone = builder.timezone;
        this.filterTemplateValues = builder.filterTemplateValues;
    }

    /**
     * Creates a new {@code AnnSearchReq} builder.
     *
     * @return the builder
     */
    public static AnnSearchReqBuilder builder() {
        return new AnnSearchReqBuilder();
    }

    /**
     * Returns the name of the vector field to search.
     *
     * @return the vector field name
     */
    public String getVectorFieldName() {
        return vectorFieldName;
    }

    /**
     * Sets the name of the vector field to search.
     *
     * @param vectorFieldName the vector field name
     */
    public void setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
    }

    /**
     * Returns the maximum number of results to return.
     *
     * @deprecated replaced by {@link #getLimit()}
     * @return the topK value
     */
    @Deprecated
    public int getTopK() {
        return topK;
    }

    /**
     * Sets the maximum number of results to return.
     *
     * @deprecated replaced by {@link #setLimit(long)}
     * @param topK the topK value
     */
    @Deprecated
    public void setTopK(int topK) {
        this.topK = topK;
        this.limit = topK;
    }

    /**
     * Returns the maximum number of results to return.
     *
     * @return the limit value
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets the maximum number of results to return.
     *
     * @param limit the limit value
     */
    public void setLimit(long limit) {
        this.limit = limit;
        this.topK = (int) limit;
    }

    /**
     * Returns the filter expression.
     *
     * @deprecated replaced by {@link #getFilter()}
     * @return the filter expression
     */
    @Deprecated
    public String getExpr() {
        return expr;
    }

    /**
     * Sets the filter expression.
     *
     * @deprecated replaced by {@link #setFilter(String)}
     * @param expr the filter expression
     */
    @Deprecated
    public void setExpr(String expr) {
        this.expr = expr;
        this.filter = expr;
    }

    /**
     * Returns the filter expression.
     *
     * @return the filter expression
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Sets the filter expression.
     *
     * @param filter the filter expression
     */
    public void setFilter(String filter) {
        this.filter = filter;
        this.expr = filter;
    }

    /**
     * Returns the query vectors.
     *
     * @return the query vectors
     */
    public List<BaseVector> getVectors() {
        return vectors;
    }

    /**
     * Sets the query vectors.
     *
     * @param vectors the query vectors
     */
    public void setVectors(List<BaseVector> vectors) {
        this.vectors = vectors;
    }

    /**
     * Returns the search parameter string.
     *
     * @return the search parameters
     */
    public String getParams() {
        return params;
    }

    /**
     * Sets the search parameter string.
     *
     * @param params the search parameters
     */
    public void setParams(String params) {
        this.params = params;
    }

    /**
     * Returns the metric type used for the search.
     *
     * @return the metric type
     */
    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    /**
     * Sets the metric type used for the search.
     *
     * @param metricType the metric type
     */
    public void setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
    }

    /**
     * Returns the timezone used for timestamp fields in the filter expression.
     *
     * @return the timezone
     */
    public String getTimezone() {
        return timezone;
    }

    /**
     * Returns the expression template values used to improve expression parsing performance.
     *
     * @return the filter template values
     */
    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
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
                ", timezone='" + timezone + '\'' +
//                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static class AnnSearchReqBuilder {
        private String vectorFieldName;
        private int topK = 0;
        private long limit = 0L;
        private String expr = "";
        private String filter = "";
        private List<BaseVector> vectors;
        private String params;
        private IndexParam.MetricType metricType = null;
        private String timezone = "";
        private Map<String, Object> filterTemplateValues = new HashMap<>();

        /**
         * Sets the name of the vector field to search.
         *
         * @param vectorFieldName the vector field name
         * @return this builder
         */
        public AnnSearchReqBuilder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @deprecated replaced by {@link #limit(long)}. {@code topK} and {@code limit} must be the
         * same value.
         * @param val the topK value
         * @return this builder
         */
        @Deprecated
        public AnnSearchReqBuilder topK(int val) {
            this.topK = val;
            this.limit = val;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param val the limit value
         * @return this builder
         */
        public AnnSearchReqBuilder limit(long val) {
            this.topK = (int) val;
            this.limit = val;
            return this;
        }

        // expr is deprecated replaced by filter, expr and filter must be the same value
        @Deprecated
        public AnnSearchReqBuilder expr(String val) {
            this.expr = val;
            this.filter = val;
            return this;
        }

        /**
         * Sets the filter expression.
         *
         * @param val the filter expression
         * @return this builder
         */
        public AnnSearchReqBuilder filter(String val) {
            this.expr = val;
            this.filter = val;
            return this;
        }

        /**
         * Sets the query vectors.
         *
         * @param vectors the query vectors
         * @return this builder
         */
        public AnnSearchReqBuilder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        /**
         * Sets the search parameter string.
         *
         * @param params the search parameters
         * @return this builder
         */
        public AnnSearchReqBuilder params(String params) {
            this.params = params;
            return this;
        }

        /**
         * Sets the metric type used for the search.
         *
         * @param metricType the metric type
         * @return this builder
         */
        public AnnSearchReqBuilder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the timezone used for timestamp fields in the filter expression.
         *
         * @param timezone the timezone
         * @return this builder
         */
        public AnnSearchReqBuilder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Sets the expression template values used to improve expression parsing performance.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */
        public AnnSearchReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Builds the {@link AnnSearchReq}.
         *
         * @return the request
         */
        public AnnSearchReq build() {
            return new AnnSearchReq(this);
        }
    }
}
