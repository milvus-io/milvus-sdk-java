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


package io.milvus.v2.service.partition.response;

import java.util.HashMap;
import java.util.Map;

/**
 * Response returned by the {@code getPartitionStats} API.
 */
public class GetPartitionStatsResp {
    private Long numOfEntities;
    private Map<String, String> stats;

    private GetPartitionStatsResp(GetPartitionStatsRespBuilder builder) {
        this.numOfEntities = builder.numOfEntities;
        this.stats = builder.stats;
    }

    /**
     * Returns the number of entities in the partition.
     *
     * @return the number of entities
     */
    public Long getNumOfEntities() {
        return numOfEntities;
    }

    /**
     * Returns the statistics of the partition.
     *
     * @return the partition statistics
     */
    public Map<String, String> getStats() {
        return stats;
    }

    /**
     * Sets the number of entities in the partition.
     *
     * @param numOfEntities the number of entities
     */
    public void setNumOfEntities(Long numOfEntities) {
        this.numOfEntities = numOfEntities;
    }

    /**
     * Sets the statistics of the partition.
     *
     * @param stats the partition statistics
     */
    public void setStats(Map<String, String> stats) {
        this.stats = stats;
    }

    @Override
    public String toString() {
        return "GetPartitionStatsResp{" +
                "numOfEntities=" + numOfEntities +
                ", stats=" + stats +
                '}';
    }

    /**
     * Creates a new builder for {@code GetPartitionStatsResp}.
     *
     * @return the builder
     */
    public static GetPartitionStatsRespBuilder builder() {
        return new GetPartitionStatsRespBuilder();
    }

    public static class GetPartitionStatsRespBuilder {
        private Long numOfEntities;
        private Map<String, String> stats = new HashMap<>();

        private GetPartitionStatsRespBuilder() {
        }

        /**
         * Sets the number of entities in the partition.
         *
         * @param numOfEntities the number of entities
         * @return this builder
         */
        public GetPartitionStatsRespBuilder numOfEntities(Long numOfEntities) {
            this.numOfEntities = numOfEntities;
            return this;
        }

        /**
         * Sets the statistics of the partition.
         *
         * @param stats the partition statistics
         * @return this builder
         */
        public GetPartitionStatsRespBuilder stats(Map<String, String> stats) {
            this.stats = stats;
            return this;
        }

        /**
         * Builds the {@code GetPartitionStatsResp}.
         *
         * @return the built response
         */
        public GetPartitionStatsResp build() {
            return new GetPartitionStatsResp(this);
        }
    }
}
