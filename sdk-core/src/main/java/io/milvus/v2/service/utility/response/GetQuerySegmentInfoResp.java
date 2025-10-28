package io.milvus.v2.service.utility.response;

import java.util.ArrayList;
import java.util.List;

public class GetQuerySegmentInfoResp {
    public static class QuerySegmentInfo {
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
        private Boolean isSorted;

        private QuerySegmentInfo(QuerySegmentInfoBuilder builder) {
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
            this.isSorted = builder.isSorted;
        }

        public static QuerySegmentInfoBuilder builder() {
            return new QuerySegmentInfoBuilder();
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

        public Long getMemSize() {
            return memSize;
        }

        public void setMemSize(Long memSize) {
            this.memSize = memSize;
        }

        public Long getNumOfRows() {
            return numOfRows;
        }

        public void setNumOfRows(Long numOfRows) {
            this.numOfRows = numOfRows;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        public Long getIndexID() {
            return indexID;
        }

        public void setIndexID(Long indexID) {
            this.indexID = indexID;
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

        public List<Long> getNodeIDs() {
            return nodeIDs;
        }

        public void setNodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
        }

        public Boolean getIsSorted() {
            return isSorted;
        }

        public void setIsSorted(Boolean isSorted) {
            this.isSorted = isSorted;
        }

        @Override
        public String toString() {
            return "QuerySegmentInfo{" +
                    "segmentID=" + segmentID +
                    ", collectionID=" + collectionID +
                    ", partitionID=" + partitionID +
                    ", memSize=" + memSize +
                    ", numOfRows=" + numOfRows +
                    ", indexName='" + indexName + '\'' +
                    ", indexID=" + indexID +
                    ", state='" + state + '\'' +
                    ", level='" + level + '\'' +
                    ", nodeIDs=" + nodeIDs +
                    ", isSorted=" + isSorted +
                    '}';
        }

        public static class QuerySegmentInfoBuilder {
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
            private Boolean isSorted;

            public QuerySegmentInfoBuilder segmentID(Long segmentID) {
                this.segmentID = segmentID;
                return this;
            }

            public QuerySegmentInfoBuilder collectionID(Long collectionID) {
                this.collectionID = collectionID;
                return this;
            }

            public QuerySegmentInfoBuilder partitionID(Long partitionID) {
                this.partitionID = partitionID;
                return this;
            }

            public QuerySegmentInfoBuilder memSize(Long memSize) {
                this.memSize = memSize;
                return this;
            }

            public QuerySegmentInfoBuilder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
                return this;
            }

            public QuerySegmentInfoBuilder indexName(String indexName) {
                this.indexName = indexName;
                return this;
            }

            public QuerySegmentInfoBuilder indexID(Long indexID) {
                this.indexID = indexID;
                return this;
            }

            public QuerySegmentInfoBuilder state(String state) {
                this.state = state;
                return this;
            }

            public QuerySegmentInfoBuilder level(String level) {
                this.level = level;
                return this;
            }

            public QuerySegmentInfoBuilder nodeIDs(List<Long> nodeIDs) {
                this.nodeIDs = nodeIDs;
                return this;
            }

            public QuerySegmentInfoBuilder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

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

    public List<QuerySegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

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

        public GetQuerySegmentInfoRespBuilder segmentInfos(List<QuerySegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        public GetQuerySegmentInfoResp build() {
            return new GetQuerySegmentInfoResp(this);
        }
    }
}
