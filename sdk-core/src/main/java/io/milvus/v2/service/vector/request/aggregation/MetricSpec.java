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

package io.milvus.v2.service.vector.request.aggregation;

import io.milvus.grpc.MetricAggSpec;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

/**
 * A metric aggregation spec of a {@code group-by} search aggregation, defining an
 * aggregation operation ({@link MetricOps}) applied to a field.
 */
public class MetricSpec {
    private final MetricOps op;
    private final String fieldName;

    private MetricSpec(MetricSpecBuilder builder) {
        if (builder.op == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation metric op must not be null.");
        }
        if (builder.fieldName == null || builder.fieldName.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation metric fieldName must not be empty.");
        }
        if (builder.op != MetricOps.COUNT && "*".equals(builder.fieldName)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "'*' is only valid for SearchAggregation count metrics.");
        }
        this.op = builder.op;
        this.fieldName = builder.fieldName;
    }

    /**
     * Creates a new builder for {@link MetricSpec}.
     *
     * @return a new builder
     */
    public static MetricSpecBuilder builder() {
        return new MetricSpecBuilder();
    }

    /**
     * Returns the aggregation operation of this metric spec.
     *
     * @return the aggregation operation
     */
    public MetricOps getOp() {
        return op;
    }

    /**
     * Returns the name of the field the aggregation operation is applied to.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    MetricAggSpec toProto() {
        return MetricAggSpec.newBuilder()
                .setOp(op.getValue())
                .setFieldName(fieldName)
                .build();
    }

    @Override
    public String toString() {
        return "MetricSpec{" +
                "op=" + op +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }

    /**
     * Builder for {@link MetricSpec}.
     */
    public static class MetricSpecBuilder {
        private MetricOps op;
        private String fieldName;

        private MetricSpecBuilder() {
        }

        /**
         * Sets the aggregation operation of this metric spec.
         *
         * @param op the aggregation operation
         * @return this builder
         */
        public MetricSpecBuilder op(MetricOps op) {
            this.op = op;
            return this;
        }

        /**
         * Sets the name of the field the aggregation operation is applied to. For count
         * metrics, {@code "*"} is accepted to count all entities.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public MetricSpecBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Builds the {@link MetricSpec}.
         *
         * @return the built metric spec
         */
        public MetricSpec build() {
            return new MetricSpec(this);
        }
    }

}
