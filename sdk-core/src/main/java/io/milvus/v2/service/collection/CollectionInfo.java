package io.milvus.v2.service.collection;

/**
 * Basic information of a collection.
 */
public class CollectionInfo {
    private String collectionName;
    private Integer shardNum;

    // Private constructor for builder
    private CollectionInfo(CollectionInfoBuilder builder) {
        this.collectionName = builder.collectionName;
        this.shardNum = builder.shardNum;
    }

    // Static method to create builder
    /**
     * Creates a new {@code CollectionInfo} builder.
     *
     * @return the builder
     */
    public static CollectionInfoBuilder builder() {
        return new CollectionInfoBuilder();
    }

    // Getter methods
    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns the number of shards of the collection.
     *
     * @return the shard number
     */
    public Integer getShardNum() {
        return shardNum;
    }

    // Setter methods
    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Sets the number of shards of the collection.
     *
     * @param shardNum the shard number
     */
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
    /**
     * Builder for {@link CollectionInfo}.
     */
    public static class CollectionInfoBuilder {
        private String collectionName;
        private Integer shardNum;

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public CollectionInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the number of shards of the collection.
         *
         * @param shardNum the shard number
         * @return this builder
         */
        public CollectionInfoBuilder shardNum(Integer shardNum) {
            this.shardNum = shardNum;
            return this;
        }

        /**
         * Builds the {@link CollectionInfo}.
         *
         * @return the collection information
         */
        public CollectionInfo build() {
            return new CollectionInfo(this);
        }
    }
}
