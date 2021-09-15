package io.milvus.param.Control;

/**
 * @author:weilongzhao
 * @time:2021/9/4 22:20
 */
public class GetPersistentSegmentInfoParam {
    private final String collectionName;

    public String getCollectionName() {
        return collectionName;
    }

    public GetPersistentSegmentInfoParam(Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public static final class Builder {
        private String collectionName;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetPersistentSegmentInfoParam build() {
            return new GetPersistentSegmentInfoParam(this);
        }
    }
}
