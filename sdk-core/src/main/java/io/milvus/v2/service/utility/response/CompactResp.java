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

package io.milvus.v2.service.utility.response;

public class CompactResp {
    private Long compactionID;

    private CompactResp(CompactRespBuilder builder) {
        this.compactionID = builder.compactionID;
    }

    public static CompactRespBuilder builder() {
        return new CompactRespBuilder();
    }

    public Long getCompactionID() {
        return compactionID;
    }

    public void setCompactionID(Long compactionID) {
        this.compactionID = compactionID;
    }

    @Override
    public String toString() {
        return "CompactResp{" +
                "compactionID=" + compactionID +
                '}';
    }

    public static class CompactRespBuilder {
        private Long compactionID = 0L;

        public CompactRespBuilder compactionID(Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        public CompactResp build() {
            return new CompactResp(this);
        }
    }
}
