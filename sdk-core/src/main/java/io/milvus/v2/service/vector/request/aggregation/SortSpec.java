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

    public static SortSpecBuilder builder() {
        return new SortSpecBuilder();
    }

    public String getFieldName() {
        return fieldName;
    }

    public AggDirection getDirection() {
        return direction;
    }

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

    public static class SortSpecBuilder {
        private String fieldName;
        private AggDirection direction;
        private Boolean nullFirst;

        private SortSpecBuilder() {
        }

        public SortSpecBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public SortSpecBuilder direction(AggDirection direction) {
            this.direction = direction;
            return this;
        }

        public SortSpecBuilder nullFirst(Boolean nullFirst) {
            this.nullFirst = nullFirst;
            return this;
        }

        public SortSpec build() {
            return new SortSpec(this);
        }
    }

}
