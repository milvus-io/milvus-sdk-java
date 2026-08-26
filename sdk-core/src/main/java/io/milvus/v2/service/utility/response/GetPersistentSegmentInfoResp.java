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
 * Response returned by the {@code getPersistentSegmentInfo} API.
 */
public class GetPersistentSegmentInfoResp {
    /**
     * Information about a persistent segment stored on disk.
     */
    public static class PersistentSegmentInfo {
        private Long segmentID;
        private Long collectionID;
        private Long partitionID;
        private String collectionName;
        private Long numOfRows;
        private String state;
        private String level;
        private Long storageVersion;
        private Boolean isSorted;

        private PersistentSegmentInfo(PersistentSegmentInfoBuilder builder) {
            this.segmentID = builder.segmentID;
            this.collectionID = builder.collectionID;
            this.partitionID = builder.partitionID;
            this.collectionName = builder.collectionName;
            this.numOfRows = builder.numOfRows;
            this.state = builder.state;
            this.level = builder.level;
            this.storageVersion = builder.storageVersion;
            this.isSorted = builder.isSorted;
        }

        public static PersistentSegmentInfoBuilder builder() {
            return new PersistentSegmentInfoBuilder();
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
            return "PersistentSegmentInfo{" +
                    "segmentID=" + segmentID +
                    ", collectionID=" + collectionID +
                    ", partitionID=" + partitionID +
                    ", collectionName='" + collectionName + '\'' +
                    ", numOfRows=" + numOfRows +
                    ", state='" + state + '\'' +
                    ", level='" + level + '\'' +
                    ", storageVersion=" + storageVersion +
                    ", isSorted=" + isSorted +
                    '}';
        }

        public static class PersistentSegmentInfoBuilder {
            private Long segmentID;
            private Long collectionID;
            private Long partitionID;
            private String collectionName;
            private Long numOfRows;
            private String state;
            private String level;
            private Long storageVersion;
            private Boolean isSorted;

            /**
             * Sets the segment ID.
             *
             * @param segmentID the segment ID
             * @return this builder
             */
            public PersistentSegmentInfoBuilder segmentID(Long segmentID) {
                this.segmentID = segmentID;
                return this;
            }

            /**
             * Sets the collection ID.
             *
             * @param collectionID the collection ID
             * @return this builder
             */
            public PersistentSegmentInfoBuilder collectionID(Long collectionID) {
                this.collectionID = collectionID;
                return this;
            }

            /**
             * Sets the partition ID.
             *
             * @param partitionID the partition ID
             * @return this builder
             */
            public PersistentSegmentInfoBuilder partitionID(Long partitionID) {
                this.partitionID = partitionID;
                return this;
            }

            /**
             * Sets the collection name.
             *
             * @param collectionName the collection name
             * @return this builder
             */
            public PersistentSegmentInfoBuilder collectionName(String collectionName) {
                this.collectionName = collectionName;
                return this;
            }

            /**
             * Sets the number of rows in the segment.
             *
             * @param numOfRows the number of rows
             * @return this builder
             */
            public PersistentSegmentInfoBuilder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
                return this;
            }

            /**
             * Sets the state of the segment.
             *
             * @param state the segment state
             * @return this builder
             */
            public PersistentSegmentInfoBuilder state(String state) {
                this.state = state;
                return this;
            }

            /**
             * Sets the level of the segment.
             *
             * @param level the segment level
             * @return this builder
             */
            public PersistentSegmentInfoBuilder level(String level) {
                this.level = level;
                return this;
            }

            /**
             * Sets the storage version of the segment.
             *
             * @param storageVersion the storage version
             * @return this builder
             */
            public PersistentSegmentInfoBuilder storageVersion(Long storageVersion) {
                this.storageVersion = storageVersion;
                return this;
            }

            /**
             * Sets whether the segment is sorted.
             *
             * @param isSorted {@code true} if the segment is sorted, {@code false} otherwise
             * @return this builder
             */
            public PersistentSegmentInfoBuilder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

            /**
             * Builds the {@code PersistentSegmentInfo}.
             *
             * @return the constructed {@code PersistentSegmentInfo}
             */
            public PersistentSegmentInfo build() {
                return new PersistentSegmentInfo(this);
            }
        }
    }

    private List<PersistentSegmentInfo> segmentInfos;

    private GetPersistentSegmentInfoResp(GetPersistentSegmentInfoRespBuilder builder) {
        this.segmentInfos = builder.segmentInfos;
    }

    public static GetPersistentSegmentInfoRespBuilder builder() {
        return new GetPersistentSegmentInfoRespBuilder();
    }

    /**
     * Returns the persistent segment information of the collection.
     *
     * @return the list of persistent segment information
     */
    public List<PersistentSegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

    /**
     * Sets the persistent segment information of the collection.
     *
     * @param segmentInfos the list of persistent segment information
     */
    public void setSegmentInfos(List<PersistentSegmentInfo> segmentInfos) {
        this.segmentInfos = segmentInfos;
    }

    @Override
    public String toString() {
        return "GetPersistentSegmentInfoResp{" +
                "segmentInfos=" + segmentInfos +
                '}';
    }

    public static class GetPersistentSegmentInfoRespBuilder {
        private List<PersistentSegmentInfo> segmentInfos = new ArrayList<>();

        /**
         * Sets the persistent segment information of the collection.
         *
         * @param segmentInfos the list of persistent segment information
         * @return this builder
         */
        public GetPersistentSegmentInfoRespBuilder segmentInfos(List<PersistentSegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        /**
         * Builds the {@code GetPersistentSegmentInfoResp}.
         *
         * @return the constructed {@code GetPersistentSegmentInfoResp}
         */
        public GetPersistentSegmentInfoResp build() {
            return new GetPersistentSegmentInfoResp(this);
        }
    }
}
