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

package io.milvus.v2.service.vector.response;

import java.util.List;

/**
 * Response returned by the {@code get} API.
 *
 * <p>{@code get} is a query-by-primary-keys convenience over {@code query}, so this class
 * inherits all members from {@link QueryResp}. The result entities are accessed through the
 * inherited {@link #getQueryResults()}; {@link #getGetResults()} is kept as a deprecated alias.
 */
public class GetResp extends QueryResp {

    private GetResp(QueryRespBuilder builder) {
        super(builder);
    }

    /**
     * Creates a new {@code GetResp} builder.
     *
     * @return the builder
     */
    public static GetRespBuilder builder() {
        return new GetRespBuilder();
    }

    /**
     * Returns the retrieved entities.
     *
     * @return the retrieved entities
     * @deprecated use {@link #getQueryResults()} instead
     */
    @Deprecated
    public List<QueryResult> getGetResults() {
        return getQueryResults();
    }

    /**
     * Sets the retrieved entities.
     *
     * @param getResults the retrieved entities
     * @deprecated use {@link #setQueryResults(List)} instead
     */
    @Deprecated
    public void setGetResults(List<QueryResult> getResults) {
        setQueryResults(getResults);
    }

    public static class GetRespBuilder extends QueryRespBuilder {

        /**
         * Sets the retrieved entities.
         *
         * @param getResults the retrieved entities
         * @return this builder
         * @deprecated use {@link #queryResults(List)} instead
         */
        @Deprecated
        public GetRespBuilder getResults(List<QueryResult> getResults) {
            queryResults(getResults);
            return this;
        }

        @Override
        public GetRespBuilder queryResults(List<QueryResult> queryResults) {
            super.queryResults(queryResults);
            return this;
        }

        @Override
        public GetRespBuilder sessionTs(long sessionTs) {
            super.sessionTs(sessionTs);
            return this;
        }

        @Override
        public GetRespBuilder cost(Long cost) {
            super.cost(cost);
            return this;
        }

        @Override
        public GetRespBuilder scannedRemoteBytes(Long scannedRemoteBytes) {
            super.scannedRemoteBytes(scannedRemoteBytes);
            return this;
        }

        @Override
        public GetRespBuilder scannedTotalBytes(Long scannedTotalBytes) {
            super.scannedTotalBytes(scannedTotalBytes);
            return this;
        }

        @Override
        public GetRespBuilder cacheHitRatio(Float cacheHitRatio) {
            super.cacheHitRatio(cacheHitRatio);
            return this;
        }

        @Override
        public GetResp build() {
            return new GetResp(this);
        }
    }
}
