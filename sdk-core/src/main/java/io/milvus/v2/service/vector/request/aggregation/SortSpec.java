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
 * A sort rule of a {@code top_hits} aggregation, specifying the field to sort by, the sort
 * direction, and how {@code null} values are handled.
 */
public class SortSpec {
    private final String fieldName;
    private final AggDirection direction;
    private final Boolean nullFirst;

    private SortSpec(SortSpecBuilder builder) {
        if (builder.fieldName == null || builder.fieldName.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "TopHitsSpec.sort fieldName cannot be empty.");
        }
        if (builder.direction == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "TopHitsSpec.sort direction must not be null.");
        }
        this.fieldName = builder.fieldName;
        this.direction = builder.direction;
        this.nullFirst = builder.nullFirst;
    }

    /**
     * Creates a new builder for {@link SortSpec}.
     *
     * @return a new builder
     */
    public static SortSpecBuilder builder() {
        return new SortSpecBuilder();
    }

    /**
     * Returns the name of the field the hits are sorted by.
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

    /**
     * Returns whether {@code null} values are sorted first.
     *
     * @return {@code true} if nulls are sorted first, or {@code null} if not specified
     */
    public Boolean getNullFirst() {
        return nullFirst;
    }

    io.milvus.grpc.SortSpec toProto() {
        io.milvus.grpc.SortSpec.Builder builder = io.milvus.grpc.SortSpec.newBuilder()
                .setFieldName(fieldName)
                .setDirection(direction.getValue());
        if (nullFirst != null) {
            builder.setNullFirst(nullFirst);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "SortSpec{" +
                "fieldName='" + fieldName + '\'' +
                ", direction=" + direction +
                ", nullFirst=" + nullFirst +
                '}';
    }

    /**
     * Builder for {@link SortSpec}.
     */
    public static class SortSpecBuilder {
        private String fieldName;
        private AggDirection direction;
        private Boolean nullFirst;

        private SortSpecBuilder() {
        }

        /**
         * Sets the name of the field the hits are sorted by.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public SortSpecBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the sort direction applied to the field.
         *
         * @param direction the sort direction
         * @return this builder
         */
        public SortSpecBuilder direction(AggDirection direction) {
            this.direction = direction;
            return this;
        }

        /**
         * Sets whether {@code null} values are sorted first.
         *
         * @param nullFirst {@code true} to sort nulls first, {@code false} to sort them last
         * @return this builder
         */
        public SortSpecBuilder nullFirst(Boolean nullFirst) {
            this.nullFirst = nullFirst;
            return this;
        }

        /**
         * Builds the {@link SortSpec}.
         *
         * @return the built sort rule
         */
        public SortSpec build() {
            return new SortSpec(this);
        }
    }

}
