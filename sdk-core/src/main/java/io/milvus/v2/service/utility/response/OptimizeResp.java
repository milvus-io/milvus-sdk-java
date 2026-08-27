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

/**
 * Response returned by the {@code optimize} API.
 */
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

    /**
     * Returns the status of the optimization.
     *
     * @return the optimization status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns the ID of the compaction triggered by the optimization.
     *
     * @return the compaction ID
     */
    public Long getCompactionId() {
        return compactionId;
    }

    /**
     * Returns the target segment size of the optimization.
     *
     * @return the target segment size
     */
    public String getTargetSize() {
        return targetSize;
    }

    /**
     * Returns the progress of the optimization.
     *
     * @return the list of progress messages
     */
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

        /**
         * Sets the status of the optimization.
         *
         * @param status the optimization status
         * @return this builder
         */
        public OptimizeRespBuilder status(String status) {
            this.status = status;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public OptimizeRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the ID of the compaction triggered by the optimization.
         *
         * @param compactionId the compaction ID
         * @return this builder
         */
        public OptimizeRespBuilder compactionId(Long compactionId) {
            this.compactionId = compactionId;
            return this;
        }

        /**
         * Sets the target segment size of the optimization.
         *
         * @param targetSize the target segment size
         * @return this builder
         */
        public OptimizeRespBuilder targetSize(String targetSize) {
            this.targetSize = targetSize;
            return this;
        }

        /**
         * Sets the progress of the optimization.
         *
         * @param progress the list of progress messages
         * @return this builder
         */
        public OptimizeRespBuilder progress(List<String> progress) {
            this.progress = progress;
            return this;
        }

        /**
         * Builds the {@code OptimizeResp}.
         *
         * @return the constructed {@code OptimizeResp}
         */
        public OptimizeResp build() {
            return new OptimizeResp(this);
        }
    }
}
