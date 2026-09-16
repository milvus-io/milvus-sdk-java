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

package io.milvus.param.highlevel.dml.response;

import java.util.List;

/**
 * Parameters for <code>insert</code> interface.
 */


public class InsertResponse {
    private final Long insertCount;
    public List<?> insertIds;

    private InsertResponse(Builder builder) {
        this.insertCount = builder.insertCount;
        this.insertIds = builder.insertIds;
    }

    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the insertCount.
     *
     * @return the insertCount
     */
    // Getter method to replace @Getter annotation
    public Long getInsertCount() {
        return insertCount;
    }

    /**
     * Returns the insertIds.
     *
     * @return the insertIds
     */


    public List<?> getInsertIds() {
        return insertIds;
    }

    /**
     * Builder for {@link InsertResponse} class to replace @Builder annotation.
     */


    public static class Builder {
        private Long insertCount;
        private List<?> insertIds;

        private Builder() {
        }

        /**
         * Sets the insertCount.
         *
         * @param insertCount the insertCount
         * @return this builder
         */


        public Builder insertCount(Long insertCount) {
            this.insertCount = insertCount;
            return this;
        }

        /**
         * Sets the insertIds.
         *
         * @param insertIds the insertIds
         * @return this builder
         */


        public Builder insertIds(List<?> insertIds) {
            this.insertIds = insertIds;
            return this;
        }

        /**
         * Builds the InsertResponse.
         *
         * @return the built InsertResponse
         */


        public InsertResponse build() {
            return new InsertResponse(this);
        }
    }
}
