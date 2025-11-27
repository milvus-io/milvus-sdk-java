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

public class GetPersistentSegmentInfoResp {
    public static class PersistentSegmentInfo {
        private Long segmentID;
        private Long collectionID;
        private Long partitionID;
        private Long numOfRows;
        private String state;
        private String level;
        private Boolean isSorted;

        private PersistentSegmentInfo(PersistentSegmentInfoBuilder builder) {
            this.segmentID = builder.segmentID;
            this.collectionID = builder.collectionID;
            this.partitionID = builder.partitionID;
            this.numOfRows = builder.numOfRows;
            this.state = builder.state;
            this.level = builder.level;
            this.isSorted = builder.isSorted;
        }

        public static PersistentSegmentInfoBuilder builder() {
            return new PersistentSegmentInfoBuilder();
        }

        public Long getSegmentID() {
            return segmentID;
        }

        public void setSegmentID(Long segmentID) {
            this.segmentID = segmentID;
        }

        public Long getCollectionID() {
            return collectionID;
        }

        public void setCollectionID(Long collectionID) {
            this.collectionID = collectionID;
        }

        public Long getPartitionID() {
            return partitionID;
        }

        public void setPartitionID(Long partitionID) {
            this.partitionID = partitionID;
        }

        public Long getNumOfRows() {
            return numOfRows;
        }

        public void setNumOfRows(Long numOfRows) {
            this.numOfRows = numOfRows;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public Boolean getIsSorted() {
            return isSorted;
        }

        public void setIsSorted(Boolean isSorted) {
            this.isSorted = isSorted;
        }

        @Override
        public String toString() {
            return "PersistentSegmentInfo{" +
                    "segmentID=" + segmentID +
                    ", collectionID=" + collectionID +
                    ", partitionID=" + partitionID +
                    ", numOfRows=" + numOfRows +
                    ", state='" + state + '\'' +
                    ", level='" + level + '\'' +
                    ", isSorted=" + isSorted +
                    '}';
        }

        public static class PersistentSegmentInfoBuilder {
            private Long segmentID;
            private Long collectionID;
            private Long partitionID;
            private Long numOfRows;
            private String state;
            private String level;
            private Boolean isSorted;

            public PersistentSegmentInfoBuilder segmentID(Long segmentID) {
                this.segmentID = segmentID;
                return this;
            }

            public PersistentSegmentInfoBuilder collectionID(Long collectionID) {
                this.collectionID = collectionID;
                return this;
            }

            public PersistentSegmentInfoBuilder partitionID(Long partitionID) {
                this.partitionID = partitionID;
                return this;
            }

            public PersistentSegmentInfoBuilder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
                return this;
            }

            public PersistentSegmentInfoBuilder state(String state) {
                this.state = state;
                return this;
            }

            public PersistentSegmentInfoBuilder level(String level) {
                this.level = level;
                return this;
            }

            public PersistentSegmentInfoBuilder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

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

    public List<PersistentSegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

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

        public GetPersistentSegmentInfoRespBuilder segmentInfos(List<PersistentSegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        public GetPersistentSegmentInfoResp build() {
            return new GetPersistentSegmentInfoResp(this);
        }
    }
}
