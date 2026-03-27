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

import java.util.List;

public class OptimizeResp {
    private String status;
    private String collectionName;
    private Long compactionId;
    private String targetSize;
    private List<String> progress;

    private OptimizeResp(OptimizeRespBuilder builder) {
        this.status = builder.status;
        this.collectionName = builder.collectionName;
        this.compactionId = builder.compactionId;
        this.targetSize = builder.targetSize;
        this.progress = builder.progress;
    }

    public static OptimizeRespBuilder builder() {
        return new OptimizeRespBuilder();
    }

    public String getStatus() {
        return status;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Long getCompactionId() {
        return compactionId;
    }

    public String getTargetSize() {
        return targetSize;
    }

    public List<String> getProgress() {
        return progress;
    }

    @Override
    public String toString() {
        return "OptimizeResp{" +
                "status='" + status + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", compactionId=" + compactionId +
                ", targetSize='" + targetSize + '\'' +
                ", progress=" + progress +
                '}';
    }

    public static class OptimizeRespBuilder {
        private String status;
        private String collectionName;
        private Long compactionId;
        private String targetSize;
        private List<String> progress;

        public OptimizeRespBuilder status(String status) {
            this.status = status;
            return this;
        }

        public OptimizeRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public OptimizeRespBuilder compactionId(Long compactionId) {
            this.compactionId = compactionId;
            return this;
        }

        public OptimizeRespBuilder targetSize(String targetSize) {
            this.targetSize = targetSize;
            return this;
        }

        public OptimizeRespBuilder progress(List<String> progress) {
            this.progress = progress;
            return this;
        }

        public OptimizeResp build() {
            return new OptimizeResp(this);
        }
    }
}
