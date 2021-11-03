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
import io.milvus.exception.ParamException;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SearchParam.vectors:
 * if is FloatVector: vectors is List<List<Float>>
 * if is BinaryVector: vectors is List<ByteBuffer>
 */
public class SearchParam {
    private final String dbName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final String metricType;
    private final String vectorFieldName;
    private final Integer topK;
    private final String expr;
    private final List<String> outFields;
    private final List<?> vectors;
    private final String params;

    private SearchParam(@Nonnull Builder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
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

    public String getMetricType() {
        return metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public Integer getTopK() {
        return topK;
    }

    public String getExpr() {
        return expr;
    }

    public List<String> getOutFields() {
        return outFields;
    }

    public List<?> getVectors() {
        return vectors;
    }

    public String getParams() {
        return params;
    }

    public static class Builder {
        private String dbName = ""; // reserved
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private Integer topK;
        private String expr;
        private List<String> outFields = new ArrayList<>();
        private List<?> vectors;
        private String params;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withPartitionNames(@Nonnull List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder withMetricType(@Nonnull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder withVectorFieldName(@Nonnull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        public Builder withTopK(@Nonnull Integer topK) {
            this.topK = topK;
            return this;
        }

        public Builder withExpr(@Nonnull String expr) {
            this.expr = expr;
            return this;
        }

        public Builder withOutFields(@Nonnull List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        public Builder withVectors(@Nonnull List<?> vectors) {
            this.vectors = vectors;
            return this;
        }


        public Builder withParams(@Nonnull String params) {
            this.params = params;
            return this;
        }

        public SearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is illegal");
            }

            if (vectors == null || vectors.isEmpty()) {
                throw new ParamException("Target vectors can not be empty");
            }

            if (vectors.get(0) instanceof List) {
                // float vectors
                List first = (List) vectors.get(0);
                if (!(first.get(0) instanceof Float)) {
                    throw new ParamException("Float vector field's value must be Lst<Float>");
                }

                int dim = first.size();
                for (int i = 1; i < vectors.size(); ++i) {
                    List temp = (List) vectors.get(i);
                    if (dim != temp.size()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else if (vectors.get(0) instanceof ByteBuffer) {
                // binary vectors
                ByteBuffer first = (ByteBuffer) vectors.get(0);
                int dim = first.position();
                for (int i = 1; i < vectors.size(); ++i) {
                    ByteBuffer temp = (ByteBuffer) vectors.get(i);
                    if (dim != temp.position()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else {
                throw new ParamException("Target vector type must be Lst<Float> or ByteBuffer");
            }

            return new SearchParam(this);
        }
    }
}
