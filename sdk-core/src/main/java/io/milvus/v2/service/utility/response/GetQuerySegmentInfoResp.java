package io.milvus.v2.service.utility.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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

        private QuerySegmentInfo(Builder builder) {
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

        public static Builder builder() {
            return new Builder();
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
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            QuerySegmentInfo that = (QuerySegmentInfo) obj;
            return new EqualsBuilder()
                    .append(segmentID, that.segmentID)
                    .append(collectionID, that.collectionID)
                    .append(partitionID, that.partitionID)
                    .append(memSize, that.memSize)
                    .append(numOfRows, that.numOfRows)
                    .append(indexName, that.indexName)
                    .append(indexID, that.indexID)
                    .append(state, that.state)
                    .append(level, that.level)
                    .append(nodeIDs, that.nodeIDs)
                    .append(isSorted, that.isSorted)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(segmentID)
                    .append(collectionID)
                    .append(partitionID)
                    .append(memSize)
                    .append(numOfRows)
                    .append(indexName)
                    .append(indexID)
                    .append(state)
                    .append(level)
                    .append(nodeIDs)
                    .append(isSorted)
                    .toHashCode();
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

        public static class Builder {
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

            public Builder segmentID(Long segmentID) {
                this.segmentID = segmentID;
                return this;
            }

            public Builder collectionID(Long collectionID) {
                this.collectionID = collectionID;
                return this;
            }

            public Builder partitionID(Long partitionID) {
                this.partitionID = partitionID;
                return this;
            }

            public Builder memSize(Long memSize) {
                this.memSize = memSize;
                return this;
            }

            public Builder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
                return this;
            }

            public Builder indexName(String indexName) {
                this.indexName = indexName;
                return this;
            }

            public Builder indexID(Long indexID) {
                this.indexID = indexID;
                return this;
            }

            public Builder state(String state) {
                this.state = state;
                return this;
            }

            public Builder level(String level) {
                this.level = level;
                return this;
            }

            public Builder nodeIDs(List<Long> nodeIDs) {
                this.nodeIDs = nodeIDs;
                return this;
            }

            public Builder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

            public QuerySegmentInfo build() {
                return new QuerySegmentInfo(this);
            }
        }
    }

    private List<QuerySegmentInfo> segmentInfos;

    private GetQuerySegmentInfoResp(Builder builder) {
        this.segmentInfos = builder.segmentInfos;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<QuerySegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

    public void setSegmentInfos(List<QuerySegmentInfo> segmentInfos) {
        this.segmentInfos = segmentInfos;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetQuerySegmentInfoResp that = (GetQuerySegmentInfoResp) obj;
        return new EqualsBuilder()
                .append(segmentInfos, that.segmentInfos)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(segmentInfos)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "GetQuerySegmentInfoResp{" +
                "segmentInfos=" + segmentInfos +
                '}';
    }

    public static class Builder {
        private List<QuerySegmentInfo> segmentInfos = new ArrayList<>();

        public Builder segmentInfos(List<QuerySegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        public GetQuerySegmentInfoResp build() {
            return new GetQuerySegmentInfoResp(this);
        }
    }
}
