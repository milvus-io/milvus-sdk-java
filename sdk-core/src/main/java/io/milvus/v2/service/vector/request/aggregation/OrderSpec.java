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

    public static OrderSpecBuilder builder() {
        return new OrderSpecBuilder();
    }

    public String getKey() {
        return key;
    }

    public AggDirection getDirection() {
        return direction;
    }

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

    public static class OrderSpecBuilder {
        private String key;
        private AggDirection direction;
        private Boolean nullFirst;

        private OrderSpecBuilder() {
        }

        public OrderSpecBuilder key(String key) {
            this.key = key;
            return this;
        }

        public OrderSpecBuilder direction(AggDirection direction) {
            this.direction = direction;
            return this;
        }

        public OrderSpecBuilder nullFirst(Boolean nullFirst) {
            this.nullFirst = nullFirst;
            return this;
        }

        public OrderSpec build() {
            return new OrderSpec(this);
        }
    }

}
