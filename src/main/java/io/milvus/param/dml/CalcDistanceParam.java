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
 * Parameters for <code>calcDistance</code> interface.
 * Note that currently only support float vectors calculation.
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

    /**
     * Builder for <code>CalcDistanceParam</code> class.
     */
    public static class Builder {
        private List<List<Float>> vectorsLeft;
        private List<List<Float>> vectorsRight;
        private MetricType metricType;

        private Builder() {
        }

        /**
         * Set a list of left side vectors. The list cannot be null or empty, each vector list cannot be null or empty.
         *
         * @param vectors a list of float list, each float list is a vector.
         * @return <code>Builder</code>
         */
        public Builder withVectorsLeft(@NonNull List<List<Float>> vectors) {
            this.vectorsLeft = vectors;
            return this;
        }

        /**
         * Set a list of right side vectors. The list cannot be null or empty, each vector list cannot be null or empty.
         *
         * @param vectors a list of float list, each float list is a vector.
         * @return <code>Builder</code>
         */
        public Builder withVectorsRight(@NonNull List<List<Float>> vectors) {
            this.vectorsRight = vectors;
            return this;
        }

        /**
         * Set metric type of calculation. Note that currently only support L2 and IP.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Verify parameters and create a new <code>CalcDistanceParam</code> instance.
         *
         * @return <code>CalcDistanceParam</code>
         */
        public CalcDistanceParam build() throws ParamException {
            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is illegal");
            }

            if (metricType != MetricType.L2 && metricType != MetricType.IP) {
                throw new ParamException("Only support L2 or IP metric type now!");
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

    /**
     * Construct a <code>String</code> by <code>CalcDistanceParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "CalcDistanceParam{ left vector count:" + vectorsLeft.size() +
                " right vector count:" + vectorsRight.size() +
                '}';
    }
}
