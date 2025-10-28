package io.milvus.v2.service.collection;

public class CollectionInfo {
    private String collectionName;
    private Integer shardNum;

    // Private constructor for builder
    private CollectionInfo(CollectionInfoBuilder builder) {
        this.collectionName = builder.collectionName;
        this.shardNum = builder.shardNum;
    }

    // Static method to create builder
    public static CollectionInfoBuilder builder() {
        return new CollectionInfoBuilder();
    }

    // Getter methods
    public String getCollectionName() {
        return collectionName;
    }

    public Integer getShardNum() {
        return shardNum;
    }

    // Setter methods
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setShardNum(Integer shardNum) {
        this.shardNum = shardNum;
    }

    @Override
    public String toString() {
        return "CollectionInfo{" +
                "collectionName='" + collectionName + '\'' +
                ", shardNum=" + shardNum +
                '}';
    }

    // Builder class
    public static class CollectionInfoBuilder {
        private String collectionName;
        private Integer shardNum;

        public CollectionInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CollectionInfoBuilder shardNum(Integer shardNum) {
            this.shardNum = shardNum;
            return this;
        }

        public CollectionInfo build() {
            return new CollectionInfo(this);
        }
    }
}
