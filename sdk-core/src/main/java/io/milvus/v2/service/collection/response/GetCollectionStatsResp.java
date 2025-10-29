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

public class GetCollectionStatsResp {
    private Long numOfEntities;

    private GetCollectionStatsResp(GetCollectionStatsRespBuilder builder) {
        this.numOfEntities = builder.numOfEntities;
    }

    public static GetCollectionStatsRespBuilder builder() {
        return new GetCollectionStatsRespBuilder();
    }

    // Getter
    public Long getNumOfEntities() {
        return numOfEntities;
    }

    // Setter
    public void setNumOfEntities(Long numOfEntities) {
        this.numOfEntities = numOfEntities;
    }

    @Override
    public String toString() {
        return "GetCollectionStatsResp{" +
                "numOfEntities=" + numOfEntities +
                '}';
    }

    public static class GetCollectionStatsRespBuilder {
        private Long numOfEntities;

        public GetCollectionStatsRespBuilder numOfEntities(Long numOfEntities) {
            this.numOfEntities = numOfEntities;
            return this;
        }

        public GetCollectionStatsResp build() {
            return new GetCollectionStatsResp(this);
        }
    }
}
