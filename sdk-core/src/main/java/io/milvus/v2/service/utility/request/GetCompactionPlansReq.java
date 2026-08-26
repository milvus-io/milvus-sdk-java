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

package io.milvus.v2.service.utility.request;

/**
 * Request parameters for the {@code getCompactionPlans} API.
 */
public class GetCompactionPlansReq {
    private Long compactionID;

    private GetCompactionPlansReq(GetCompactionPlansReqBuilder builder) {
        this.compactionID = builder.compactionID;
    }

    public static GetCompactionPlansReqBuilder builder() {
        return new GetCompactionPlansReqBuilder();
    }

    /**
     * Returns the compaction ID.
     *
     * @return the compaction ID
     */
    public Long getCompactionID() {
        return compactionID;
    }

    /**
     * Sets the compaction ID.
     *
     * @param compactionID the compaction ID
     */
    public void setCompactionID(Long compactionID) {
        this.compactionID = compactionID;
    }

    @Override
    public String toString() {
        return "GetCompactionPlansReq{" +
                "compactionID=" + compactionID +
                '}';
    }

    public static class GetCompactionPlansReqBuilder {
        private Long compactionID;

        /**
         * Sets the compaction ID.
         *
         * @param compactionID the compaction ID
         * @return this builder
         */
        public GetCompactionPlansReqBuilder compactionID(Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        /**
         * Builds the {@code GetCompactionPlansReq}.
         *
         * @return the constructed {@code GetCompactionPlansReq}
         */
        public GetCompactionPlansReq build() {
            return new GetCompactionPlansReq(this);
        }
    }
}
