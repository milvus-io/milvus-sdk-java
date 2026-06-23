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

package io.milvus.v2.service.collection.response;

import java.util.HashMap;
import java.util.Map;

public class GetCollectionStatsResp {
    private Long numOfEntities;
    private Map<String, String> stats;

    private GetCollectionStatsResp(GetCollectionStatsRespBuilder builder) {
        this.numOfEntities = builder.numOfEntities;
        this.stats = builder.stats;
    }

    public static GetCollectionStatsRespBuilder builder() {
        return new GetCollectionStatsRespBuilder();
    }

    // Getter
    public Long getNumOfEntities() {
        return numOfEntities;
    }

    public Map<String, String> getStats() {
        return stats;
    }

    // Setter
    public void setNumOfEntities(Long numOfEntities) {
        this.numOfEntities = numOfEntities;
    }

    public void setStats(Map<String, String> stats) {
        this.stats = stats;
    }

    @Override
    public String toString() {
        return "GetCollectionStatsResp{" +
                "numOfEntities=" + numOfEntities +
                ", stats=" + stats +
                '}';
    }

    public static class GetCollectionStatsRespBuilder {
        private Long numOfEntities;
        private Map<String, String> stats = new HashMap<>();

        public GetCollectionStatsRespBuilder numOfEntities(Long numOfEntities) {
            this.numOfEntities = numOfEntities;
            return this;
        }

        public GetCollectionStatsRespBuilder stats(Map<String, String> stats) {
            this.stats = stats;
            return this;
        }

        public GetCollectionStatsResp build() {
            return new GetCollectionStatsResp(this);
        }
    }
}
