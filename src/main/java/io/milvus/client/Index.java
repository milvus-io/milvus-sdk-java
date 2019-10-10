package io.milvus.client;

import javax.annotation.Nonnull;

public class Index {
    private final IndexType indexType;
    private final int nList;

    public static class Builder {
        // Optional parameters - initialized to default values
        private IndexType indexType = IndexType.FLAT;
        private int nList = 16384;

        public Builder withIndexType(@Nonnull IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder withNList(int nList) {
            this.nList = nList;
            return this;
        }

        public Index build() {
            return new Index(this);
        }
    }

    private Index(@Nonnull Builder builder) {
        this.indexType = builder.indexType;
        this.nList = builder.nList;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public int getNList() {
        return nList;
    }
}
