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

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

/**
 * An ordering rule of a {@code group-by} search aggregation, specifying the sort key,
 * the sort direction, and how {@code null} values are handled.
 */
public class OrderSpec {
    private final String key;
    private final AggDirection direction;
    private final Boolean nullFirst;

    private OrderSpec(OrderSpecBuilder builder) {
        if (builder.key == null || builder.key.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation.order key must not be empty.");
        }
        if (builder.direction == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation.order direction must not be null.");
        }
        this.key = builder.key;
        this.direction = builder.direction;
        this.nullFirst = builder.nullFirst;
    }

    /**
     * Creates a new builder for {@link OrderSpec}.
     *
     * @return a new builder
     */
    public static OrderSpecBuilder builder() {
        return new OrderSpecBuilder();
    }

    /**
     * Returns the sort key, which is a metric alias or one of the special keys
     * {@code _count} or {@code _key}.
     *
     * @return the sort key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the sort direction of the ordering rule.
     *
     * @return the sort direction
     */
    public AggDirection getDirection() {
        return direction;
    }

    /**
     * Returns whether {@code null} values are ordered first.
     *
     * @return {@code true} if nulls are ordered first, or {@code null} if not specified
     */
    public Boolean getNullFirst() {
        return nullFirst;
    }

    io.milvus.grpc.OrderSpec toProto() {
        io.milvus.grpc.OrderSpec.Builder builder = io.milvus.grpc.OrderSpec.newBuilder()
                .setKey(key)
                .setDirection(direction.getValue());
        if (nullFirst != null) {
            builder.setNullFirst(nullFirst);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "OrderSpec{" +
                "key='" + key + '\'' +
                ", direction=" + direction +
                ", nullFirst=" + nullFirst +
                '}';
    }

    /**
     * Builder for {@link OrderSpec}.
     */
    public static class OrderSpecBuilder {
        private String key;
        private AggDirection direction;
        private Boolean nullFirst;

        private OrderSpecBuilder() {
        }

        /**
         * Sets the sort key, which is a metric alias or one of the special keys
         * {@code _count} or {@code _key}.
         *
         * @param key the sort key
         * @return this builder
         */
        public OrderSpecBuilder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Sets the sort direction of the ordering rule.
         *
         * @param direction the sort direction
         * @return this builder
         */
        public OrderSpecBuilder direction(AggDirection direction) {
            this.direction = direction;
            return this;
        }

        /**
         * Sets whether {@code null} values are ordered first.
         *
         * @param nullFirst {@code true} to order nulls first, {@code false} to order them last
         * @return this builder
         */
        public OrderSpecBuilder nullFirst(Boolean nullFirst) {
            this.nullFirst = nullFirst;
            return this;
        }

        /**
         * Builds the {@link OrderSpec}.
         *
         * @return the built ordering rule
         */
        public OrderSpec build() {
            return new OrderSpec(this);
        }
    }

}
