package io.milvus.v2.service.utility.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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

        private PersistentSegmentInfo(Builder builder) {
            this.segmentID = builder.segmentID;
            this.collectionID = builder.collectionID;
            this.partitionID = builder.partitionID;
            this.numOfRows = builder.numOfRows;
            this.state = builder.state;
            this.level = builder.level;
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
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            PersistentSegmentInfo that = (PersistentSegmentInfo) obj;
            return new EqualsBuilder()
                    .append(segmentID, that.segmentID)
                    .append(collectionID, that.collectionID)
                    .append(partitionID, that.partitionID)
                    .append(numOfRows, that.numOfRows)
                    .append(state, that.state)
                    .append(level, that.level)
                    .append(isSorted, that.isSorted)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(segmentID)
                    .append(collectionID)
                    .append(partitionID)
                    .append(numOfRows)
                    .append(state)
                    .append(level)
                    .append(isSorted)
                    .toHashCode();
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

        public static class Builder {
            private Long segmentID;
            private Long collectionID;
            private Long partitionID;
            private Long numOfRows;
            private String state;
            private String level;
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

            public Builder numOfRows(Long numOfRows) {
                this.numOfRows = numOfRows;
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

            public Builder isSorted(Boolean isSorted) {
                this.isSorted = isSorted;
                return this;
            }

            public PersistentSegmentInfo build() {
                return new PersistentSegmentInfo(this);
            }
        }
    }

    private List<PersistentSegmentInfo> segmentInfos;

    private GetPersistentSegmentInfoResp(Builder builder) {
        this.segmentInfos = builder.segmentInfos;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<PersistentSegmentInfo> getSegmentInfos() {
        return segmentInfos;
    }

    public void setSegmentInfos(List<PersistentSegmentInfo> segmentInfos) {
        this.segmentInfos = segmentInfos;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetPersistentSegmentInfoResp that = (GetPersistentSegmentInfoResp) obj;
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
        return "GetPersistentSegmentInfoResp{" +
                "segmentInfos=" + segmentInfos +
                '}';
    }

    public static class Builder {
        private List<PersistentSegmentInfo> segmentInfos = new ArrayList<>();

        public Builder segmentInfos(List<PersistentSegmentInfo> segmentInfos) {
            this.segmentInfos = segmentInfos;
            return this;
        }

        public GetPersistentSegmentInfoResp build() {
            return new GetPersistentSegmentInfoResp(this);
        }
    }
}
