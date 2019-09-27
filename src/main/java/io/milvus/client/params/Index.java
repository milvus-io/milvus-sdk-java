package io.milvus.client.params;

public class Index {
    private final IndexType indexType;
    private final int nList;

    public static class Builder {
        // Optional parameters - initialized to default values
        private IndexType indexType = IndexType.FLAT;
        private int nList = 16384;

        public Builder setIndexType(IndexType val) {
            indexType = val;
            return this;
        }

        public Builder setNList(int val) {
            nList = val;
            return this;
        }

        public Index build() {
            return new Index(this);
        }
    }

    private Index(Builder builder) {
        this.indexType = builder.indexType;
        this.nList = builder.nList;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public int getnNList() {
        return nList;
    }
}
