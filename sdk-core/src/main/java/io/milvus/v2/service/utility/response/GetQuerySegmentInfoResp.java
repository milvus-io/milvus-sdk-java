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

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code getQuerySegmentInfo} API.
 */
public class GetQuerySegmentInfoResp {
    /**
     * Information about a query segment loaded in the query nodes.
     */
    public static class QuerySegmentInfo {
        private String collectionName;
        private Long segmentID;
        private Long collectionID;
        private Long partitionID;
        private Long memSize;
        private Long numOfRows;
        private String indexName;
        private Long indexID;
        private String state;
        private String level;
        private List<Long> nodeIDs;
        private Long storageVersion;
        private Boolean isSorted;

        private QuerySegmentInfo(QuerySegmentInfoBuilder builder) {
            this.collectionName = builder.collectionName;
            this.segmentID = builder.segmentID;
            this.collectionID = builder.collectionID;
            this.partitionID = builder.partitionID;
            this.memSize = builder.memSize;
            this.numOfRows = builder.numOfRows;
            this.indexName = builder.indexName;
            this.indexID = builder.indexID;
            this.state = builder.state;
            this.level = builder.level;
            this.nodeIDs = builder.nodeIDs;
            this.storageVersion = builder.storageVersion;
            this.isSorted = builder.isSorted;
        }

        public static QuerySegmentInfoBuilder builder() {
            return new QuerySegmentInfoBuilder();
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
         * Sets the collection name.
         *
         * @param collectionName the collection name
         */
        public void setCollectionName(String collectionName) {
            this.collectionName = collectionName;
        }

        /**
         * Returns the segment ID.
         *
         * @return the segment ID
         */
        public Long getSegmentID() {
            return segmentID;
        }

        /**
         * Sets the segment ID.
         *
         * @param segmentID the segment ID
         */
        public void setSegmentID(Long segmentID) {
            this.segmentID = segmentID;
        }

        /**
         * Returns the collection ID.
         *
         * @return the collection ID
         */
        public Long getCollectionID() {
            return collectionID;
        }

        /**
         * Sets the collection ID.
         *
         * @param collectionID the collection ID
         */
        public void setCollectionID(Long collectionID) {
            this.collectionID = collectionID;
        }

        /**
         * Returns the partition ID.
         *
         * @return the partition ID
         */
        public Long getPartitionID() {
            return partitionID;
        }

        /**
         * Sets the partition ID.
         *
         * @param partitionID the partition ID
         */
        public void setPartitionID(Long partitionID) {
            this.partitionID = partitionID;
        }

        /**
         * Returns the memory size occupied by the segment.
         *
         * @return the memory size
         */
        public Long getMemSize() {
            return memSize;
        }

        /**
         * Sets the memory size occupied by the segment.
         *
         * @param memSize the memory size
         */
        public void setMemSize(Long memSize) {
            this.memSize = memSize;
        }

        /**
         * Returns the number of rows in the segment.
         *
         * @return the number of rows
         */
        public Long getNumOfRows() {
            return numOfRows;
        }

        /**
         * Sets the number of rows in the segment.
         *
         * @param numOfRows the number of rows
         */
        public void setNumOfRows(Long numOfRows) {
            this.numOfRows = numOfRows;
        }

        /**
         * Returns the name of the index built on the segment.
         *
         * @return the index name
         */
        public String getIndexName() {
            return indexName;
        }

        /**
         * Sets the name of the index built on the segment.
         *
         * @param indexName the index name
         */
        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        /**
         * Returns the ID of the index built on the segment.
         *
         * @return the index ID
         */
        public Long getIndexID() {
            return indexID;
        }

        /**
         * Sets the ID of the index built on the segment.
         *
         * @param indexID the index ID
         */
        public void setIndexID(Long indexID) {
            this.indexID = indexID;
        }

        /**
         * Returns the state of the segment.
         *
         * @return the segment state
         */
        public String getState() {
            return state;
        }

        /**
         * Sets the state of the segment.
         *
         * @param state the segment state
         */
        public void setState(String state) {
            this.state = state;
        }

        /**
         * Returns the level of the segment.
         *
         * @return the segment level
         */
        public String getLevel() {
            return level;
        }

        /**
         * Sets the level of the segment.
         *
         * @param level the segment level
         */
        public void setLevel(String level) {
            this.level = level;
        }

        /**
         * Returns the IDs of the query nodes that loaded the segment.
         *
         * @return the list of query node IDs
         */
        public List<Long> getNodeIDs() {
            return nodeIDs;
        }

        /**
         * Sets the IDs of the query nodes that loaded the segment.
         *
         * @param nodeIDs the list of query node IDs
         */
        public void setNodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
        }

        /**
         * Returns the storage version of the segment.
         *
         * @return the storage version
         */
        public Long getStorageVersion() {
            return storageVersion;
        }

        /**
         * Sets the storage version of the segment.
         *
         * @param storageVersion the storage version
         */
        public void setStorageVersion(Long storageVersion) {
            this.storageVersion = storageVersion;
        }

        /**
         * Returns whether the segment is sorted.
         *
         * @return {@code true} if the segment is sorted, {@code false} otherwise
         */
        public Boolean getIsSorted() {
            return isSorted;
        }

        /**
         * Sets whether the segment is sorted.
         *
         * @param isSorted {@code true} if the segment is sorted, {@code false} otherwise
         */
        public void setIsSorted(Boolean isSorted) {
            this.isSorted = isSorted;
        }

        @Override
        public String toString() {
            return "QuerySegmentInfo{" +
                    "collectionName='" + collectionName + '\'' +
                    ", segmentID=" + segmentID +
                    ", collectionID=" + collectionID +
                    ", partitionID=" + partitionID +
                    ", memSize=" + memSize +
                    ", numOfRows=" + numOfRows +
                    ", indexName='" + indexName + '\'' +
                    ", indexID=" + indexID +
                    ", state='" + state + '\'' +
                    ", level='" + level + '\'' +
                    ", nodeIDs=" + nodeIDs +
                    ", storageVersion=" + storageVersion +
                    ", isSorted=" + isSorted +
                    '}';
        }

        public static class QuerySegmentInfoBuilder {
            private String collectionName;
            private Long segmentID;
            private Long collectionID;
            private Long partitionID;
            private Long memSize;
            private Long numOfRows;
            private String indexName;
            private Long indexID;
            private String state;
            private String level;
            private List<Long> nodeIDs = new ArrayList<>();
            private Long storageVersion;
            private Boolean isSorted;

            /**
             * Sets the collection name.
             *
             * @param collectionName the collection name
             * @return this builder
             */
            public QuerySegmentInfoBuilder collectionName(String collectionName) {
                this.collectionName = collectionName;
                return this;
            }

            /**
             * Sets the segment ID.
             *
             * @param segmentID the segment ID
             * @return this builder
             */
            public QuerySegmentInfoBuilder segmentID(Long segmentID) {
                this.segmentID = segmentID;
                return this;
            }

            /**
             * Sets the collection ID.
             *
             * @param collectionID the collection ID
             * @return this builder
             */
            public QuerySegmentInfoBuilder collectionID(Long collectionID) {
                this.collectionID = collectionID;
                return this;
            }

            /**
             * Sets the partition ID.
             *
             * @param partitionID the partition ID
             * @return this builder
             */
            public QuerySegmentInfoBuilder partitionID(Long partitionID) {
                this.partitionID = partitionID;
                return this;
            }

            /**
             * Sets the memory size occupied by the segment.
             *
             * @param memSize the memory size
             * @return this builder
             */
            public QuerySegmentInfoBuilder memSize(Long memSize) {
                this.memSize = memSize;
                return this;
            }

            /**
             * Sets the number of rows in the segment.
             *
             * @param numOfRows the number of rows
             * @return this builder
             */
            public QuerySegmentInfoBuilder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
                return this;
            }

            /**
             * Sets the name of the index built on the segment.
             *
             * @param indexName the index name
             * @return this builder
             */
            public QuerySegmentInfoBuilder indexName(String indexName) {
                this.indexName = indexName;
                return this;
            }

            /**
             * Sets the ID of the index built on the segment.
             *
             * @param indexID the index ID
             * @return this builder
             */
            public QuerySegmentInfoBuilder indexID(Long indexID) {
                this.indexID = indexID;
                return this;
            }

            /**
             * Sets the state of the segment.
             *
             * @param state the segment state
             * @return this builder
             */
            public QuerySegmentInfoBuilder state(String state) {
                this.state = state;
                return this;
            }

            /**
             * Sets the level of the segment.
             *
             * @param level the segment level
             * @return this builder
             */
            public QuerySegmentInfoBuilder level(String level) {
                this.level = level;
                return this;
            }

            /**
             * Sets the IDs of the query nodes that loaded the segment.
             *
             * @param nodeIDs the list of query node IDs
             * @return this builder
             */
            public QuerySegmentInfoBuilder nodeIDs(List<Long> nodeIDs) {
                this.nodeIDs = nodeIDs;
                return this;
            }

            /**
             * Sets the storage version of the segment.
             *
             * @param storageVersion the storage version
             * @return this builder
             */
            public QuerySegmentInfoBuilder storageVersion(Long storageVersion) {
                this.storageVersion = storageVersion;
                return this;
            }

            /**
             * Sets whether the segment is sorted.
             *
             * @param isSorted {@code true} if the segment is sorted, {@code false} otherwise
             * @return this builder
             */
            public QuerySegmentInfoBuilder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

            /**
             * Builds the {@code QuerySegmentInfo}.
             *
             * @return the constructed {@code QuerySegmentInfo}
             */
            public QuerySegmentInfo build() {
                return new QuerySegmentInfo(this);
            }
        }
    }

    private List<QuerySegmentInfo> segmentInfos;

    private GetQuerySegmentInfoResp(GetQuerySegmentInfoRespBuilder builder) {
        this.segmentInfos = builder.segmentInfos;
    }

    public static GetQuerySegmentInfoRespBuilder builder() {
        return new GetQuerySegmentInfoRespBuilder();
    }

    /**
     * Returns the query segment information of the collection.
     *
     * @return the list of query segment information
     */
    public List<QuerySegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

    /**
     * Sets the query segment information of the collection.
     *
     * @param segmentInfos the list of query segment information
     */
    public void setSegmentInfos(List<QuerySegmentInfo> segmentInfos) {
        this.segmentInfos = segmentInfos;
    }

    @Override
    public String toString() {
        return "GetQuerySegmentInfoResp{" +
                "segmentInfos=" + segmentInfos +
                '}';
    }

    public static class GetQuerySegmentInfoRespBuilder {
        private List<QuerySegmentInfo> segmentInfos = new ArrayList<>();

        /**
         * Sets the query segment information of the collection.
         *
         * @param segmentInfos the list of query segment information
         * @return this builder
         */
        public GetQuerySegmentInfoRespBuilder segmentInfos(List<QuerySegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        /**
         * Builds the {@code GetQuerySegmentInfoResp}.
         *
         * @return the constructed {@code GetQuerySegmentInfoResp}
         */
        public GetQuerySegmentInfoResp build() {
            return new GetQuerySegmentInfoResp(this);
        }
    }
}
