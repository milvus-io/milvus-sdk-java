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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The {@code top_hits} aggregation spec of a {@code group-by} search aggregation, which
 * returns a fixed number of the most relevant hits of each group together with an
 * optional sort rule.
 */
public class TopHitsSpec {
    private final long size;
    private final List<SortSpec> sort;

    private TopHitsSpec(TopHitsSpecBuilder builder) {
        if (builder.size <= 0) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "TopHitsSpec.size must be a positive integer.");
        }
        this.size = builder.size;
        this.sort = Collections.unmodifiableList(new ArrayList<>(builder.sort));
    }

    /**
     * Creates a new builder for {@link TopHitsSpec}.
     *
     * @return a new builder
     */
    public static TopHitsSpecBuilder builder() {
        return new TopHitsSpecBuilder();
    }

    /**
     * Returns the number of hits to return per group.
     *
     * @return the number of hits
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns the sort rules applied to the hits of each group.
     *
     * @return the sort rules
     */
    public List<SortSpec> getSort() {
        return sort;
    }

    public io.milvus.grpc.TopHitsSpec toProto() {
        io.milvus.grpc.TopHitsSpec.Builder builder = io.milvus.grpc.TopHitsSpec.newBuilder().setSize(size);
        for (SortSpec item : sort) {
            builder.addSort(item.toProto());
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "TopHitsSpec{" +
                "size=" + size +
                ", sort=" + sort +
                '}';
    }

    /**
     * Builder for {@link TopHitsSpec}.
     */
    public static class TopHitsSpecBuilder {
        private long size;
        private final List<SortSpec> sort = new ArrayList<>();

        private TopHitsSpecBuilder() {
        }

        /**
         * Sets the number of hits to return per group.
         *
         * @param size the number of hits
         * @return this builder
         */
        public TopHitsSpecBuilder size(long size) {
            this.size = size;
            return this;
        }

        /**
         * Replaces the sort rules applied to the hits of each group.
         *
         * @param sort the sort rules
         * @return this builder
         */
        public TopHitsSpecBuilder sort(List<SortSpec> sort) {
            this.sort.clear();
            if (sort != null) {
                sort.forEach(this::addSort);
            }
            return this;
        }

        /**
         * Adds a single sort rule to the top-hits spec.
         *
         * @param sort the sort rule
         * @return this builder
         * @throws MilvusClientException if the sort rule is {@code null}
         */
        public TopHitsSpecBuilder addSort(SortSpec sort) {
            if (sort == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "TopHitsSpec.sort entry cannot be null.");
            }
            this.sort.add(sort);
            return this;
        }

        /**
         * Builds the {@link TopHitsSpec}.
         *
         * @return the built top-hits spec
         */
        public TopHitsSpec build() {
            return new TopHitsSpec(this);
        }
    }
}
