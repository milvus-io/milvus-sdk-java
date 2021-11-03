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

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.MetricType;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * currently only support float vectors calculation
 */
public class CalcDistanceParam {
    private final List<List<Float>> vectors_left;
    private final List<List<Float>> vectors_right;
    private final String metricType;

    private CalcDistanceParam(@Nonnull Builder builder) {
        this.vectors_left = builder.vectors_left;
        this.vectors_right = builder.vectors_right;
        this.metricType = builder.metricType.name();
    }

    public List<List<Float>> getVectorsLeft() {
        return vectors_left;
    }

    public List<List<Float>> getVectorsRight() {
        return vectors_right;
    }

    public String getMetricType() {
        return metricType;
    }

    public static class Builder {
        private List<List<Float>> vectors_left;
        private List<List<Float>> vectors_right;
        private MetricType metricType;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withVectorsLeft(@Nonnull List<List<Float>> vectors) {
            this.vectors_left = vectors;
            return this;
        }

        public Builder withVectorsRight(@Nonnull List<List<Float>> vectors) {
            this.vectors_right = vectors;
            return this;
        }

        public Builder withMetricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public CalcDistanceParam build() throws ParamException {
            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is illegal");
            }

            if (vectors_left == null || vectors_left.isEmpty()) {
                throw new ParamException("Left vectors can not be empty");
            }

            int count = vectors_left.get(0).size();
            for (List<Float> vector : vectors_left) {
                if (vector.size() != count) {
                    throw new ParamException("Left vector's dimension must be equal");
                }
            }

            if (vectors_right == null || vectors_right.isEmpty()) {
                throw new ParamException("Right vectors can not be empty");
            }

            count = vectors_right.get(0).size();
            for (List<Float> vector : vectors_right) {
                if (vector.size() != count) {
                    throw new ParamException("Right vector's dimension must be equal");
                }
            }

            return new CalcDistanceParam(this);
        }
    }
}
