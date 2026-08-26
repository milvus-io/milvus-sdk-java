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
 * An ordering rule of a {@code group-by} search aggregation that sorts the returned
 * groups by a field, either in ascending or descending order.
 */
public class OrderByField {
    private final String fieldName;
    private final AggDirection direction;

    private OrderByField(OrderByFieldBuilder builder) {
        if (builder.fieldName == null || builder.fieldName.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "orderByFields.fieldName cannot be empty.");
        }
        if (builder.direction == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "orderByFields.direction must not be null.");
        }
        this.fieldName = builder.fieldName;
        this.direction = builder.direction;
    }

    /**
     * Creates a new builder for {@link OrderByField}.
     *
     * @return a new builder
     */
    public static OrderByFieldBuilder builder() {
        return new OrderByFieldBuilder();
    }

    /**
     * Returns the name of the field the groups are ordered by.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the sort direction applied to the field.
     *
     * @return the sort direction
     */
    public AggDirection getDirection() {
        return direction;
    }

    @Override
    public String toString() {
        return "OrderByField{" +
                "fieldName='" + fieldName + '\'' +
                ", direction=" + direction +
                '}';
    }

    /**
     * Builder for {@link OrderByField}.
     */
    public static class OrderByFieldBuilder {
        private String fieldName;
        private AggDirection direction = AggDirection.ASC;

        /**
         * Sets the name of the field the groups are ordered by.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public OrderByFieldBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the sort direction applied to the field. Defaults to {@link AggDirection#ASC}.
         *
         * @param direction the sort direction
         * @return this builder
         */
        public OrderByFieldBuilder direction(AggDirection direction) {
            this.direction = direction;
            return this;
        }

        /**
         * Builds the {@link OrderByField}.
         *
         * @return the built ordering rule
         */
        public OrderByField build() {
            return new OrderByField(this);
        }
    }
}
