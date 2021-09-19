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

package io.milvus.param.dml;

import com.google.common.collect.Lists;
import io.milvus.grpc.DslType;
import io.milvus.param.MetricType;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchParam {
    private final String dbName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final MetricType metricType;
    private final String vectorFieldName;
    private final Integer topK;
    private final DslType dslType;
    private final String dsl;
    private final List<String> outFields;
    private final List<List<Float>> vectors;
    private final Map<String, String> params;

    private SearchParam(@Nonnull Builder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.dslType = builder.dslType;
        this.dsl = builder.dsl;
        this.outFields = builder.outFields;
        this.vectors = builder.vectors;
        this.params = builder.params;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public Integer getTopK() {
        return topK;
    }

    public DslType getDslType() {
        return dslType;
    }

    public String getDsl() {
        return dsl;
    }

    public List<String> getOutFields() {
        return outFields;
    }

    public List<List<Float>> getVectors() {
        return vectors;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public static class Builder {
        private String dbName = "";
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private Integer topK;
        private DslType dslType = DslType.BoolExprV1;
        private String dsl;
        private List<String> outFields = new ArrayList<>();
        private List<List<Float>> vectors;
        private Map<String, String> params = new HashMap<>();

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setDbName(@Nonnull String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder setCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder setPartitionNames(@Nonnull List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder setMetricType(@Nonnull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder setVectorFieldName(@Nonnull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        public Builder setTopK(@Nonnull Integer topK) {
            this.topK = topK;
            return this;
        }

        public Builder setDslType(@Nonnull DslType dslType) {
            this.dslType = dslType;
            return this;
        }

        public Builder setDsl(@Nonnull String dsl) {
            this.dsl = dsl;
            return this;
        }

        public Builder setOutFields(@Nonnull List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        public Builder setVectors(@Nonnull List<List<Float>> vectors) {
            this.vectors = vectors;
            return this;
        }


        public Builder setParams(@Nonnull Map<String, String> params) {
            this.params = params;
            return this;
        }

        public SearchParam build() {
            return new SearchParam(this);
        }
    }
}
