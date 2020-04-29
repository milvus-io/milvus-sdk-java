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

package io.milvus.client;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Contains parameters for <code>searchByID</code> */
public class SearchByIDParam {

    private final String collectionName;
    private final List<String> partitionTags;
    private final List<Long> ids;
    private final long topK;
    private final String paramsInJson;

    private SearchByIDParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionTags = builder.partitionTags;
        this.ids = builder.ids;
        this.topK = builder.topK;
        this.paramsInJson = builder.paramsInJson;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getPartitionTags() {
        return partitionTags;
    }

    public List<Long> getIds() {
        return ids;
    }

    public long getTopK() {
        return topK;
    }

    public String getParamsInJson() {
        return paramsInJson;
    }

    /** Builder for <code>SearchByIDParam</code> */
    public static class Builder {
        // Required parameters
        private final String collectionName;

        // Optional parameters - initialized to default values
        private List<String> partitionTags = new ArrayList<>();
        private List<Long> ids = new ArrayList<>();
        private long topK = 1024;
        private String paramsInJson;

        /** @param collectionName collection to search from */
        public Builder(@Nonnull String collectionName) {
            this.collectionName = collectionName;
        }

        /**
         * Search vectors IDs. Default to an empty <code>List</code>
         *
         * @param ids IDs of vectors
         * @return <code>Builder</code>
         */
        public Builder withIDs(@Nonnull List<Long> ids) {
            this.ids = ids;
            return this;
        }

        /**
         * Optional. Search vectors with corresponding <code>partitionTags</code>. Default to an empty
         * <code>List</code>
         *
         * @param partitionTags a <code>List</code> of partition tags
         * @return <code>Builder</code>
         */
        public Builder withPartitionTags(@Nonnull List<String> partitionTags) {
            this.partitionTags = partitionTags;
            return this;
        }

        /**
         * Optional. Limits search result to <code>topK</code>. Default to 1024.
         *
         * @param topK a topK number
         * @return <code>Builder</code>
         */
        public Builder withTopK(long topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Optional. Default to empty <code>String</code>. Search parameters are different for different
         * index types. Refer to <a
         * href="https://milvus.io/docs/v0.8.0/guides/milvus_operation.md">https://milvus.io/docs/v0.8.0/guides/milvus_operation.md</a>
         * for more information.
         *
         * <pre>
         *   FLAT/IVFLAT/SQ8/IVFPQ: {"nprobe": 32}
         *   nprobe range:[1,999999]
         *
         *   NSG: {"search_length": 100}
         *   search_length range:[10, 300]
         *
         *   HNSW: {"ef": 64}
         *   ef range:[topk, 4096]
         *
         *   ANNOY: {search_k", 0.05 * totalDataCount}
         *   search_k range: none
         * </pre>
         *
         * @param paramsInJson extra parameters in JSON format
         * @return <code>Builder</code>
         */
        public SearchByIDParam.Builder withParamsInJson(@Nonnull String paramsInJson) {
            this.paramsInJson = paramsInJson;
            return this;
        }

        public SearchByIDParam build() {
            return new SearchByIDParam(this);
        }
    }
}
