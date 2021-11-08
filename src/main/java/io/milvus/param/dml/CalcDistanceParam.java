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
import io.milvus.param.MetricType;

import lombok.Getter;
import lombok.NonNull;
import java.util.List;

/**
 * currently only support float vectors calculation
 */
@Getter
public class CalcDistanceParam {
    private final List<List<Float>> vectorsLeft;
    private final List<List<Float>> vectorsRight;
    private final String metricType;

    private CalcDistanceParam(@NonNull Builder builder) {
        this.vectorsLeft = builder.vectorsLeft;
        this.vectorsRight = builder.vectorsRight;
        this.metricType = builder.metricType.name();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private List<List<Float>> vectorsLeft;
        private List<List<Float>> vectorsRight;
        private MetricType metricType;

        private Builder() {
        }

        public Builder withVectorsLeft(@NonNull List<List<Float>> vectors) {
            this.vectorsLeft = vectors;
            return this;
        }

        public Builder withVectorsRight(@NonNull List<List<Float>> vectors) {
            this.vectorsRight = vectors;
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

            if (vectorsLeft == null || vectorsLeft.isEmpty()) {
                throw new ParamException("Left vectors can not be empty");
            }

            int count = vectorsLeft.get(0).size();
            for (List<Float> vector : vectorsLeft) {
                if (vector.size() != count) {
                    throw new ParamException("Left vector's dimension must be equal");
                }
            }

            if (vectorsRight == null || vectorsRight.isEmpty()) {
                throw new ParamException("Right vectors can not be empty");
            }

            count = vectorsRight.get(0).size();
            for (List<Float> vector : vectorsRight) {
                if (vector.size() != count) {
                    throw new ParamException("Right vector's dimension must be equal");
                }
            }

            return new CalcDistanceParam(this);
        }
    }

    @Override
    public String toString() {
        return "CalcDistanceParam{" +
                '}';
    }
}
