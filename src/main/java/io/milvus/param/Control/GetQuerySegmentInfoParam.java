package io.milvus.param.Control;

/**
 * @author:weilongzhao
 * @time:2021/9/4 23:01
 */
public class GetQuerySegmentInfoParam {
    private final String collectionName;

    public String getCollectionName() {
        return collectionName;
    }

    public GetQuerySegmentInfoParam(GetQuerySegmentInfoParam.Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public static final class Builder {
        private String collectionName;

        private Builder() {
        }

        public static GetQuerySegmentInfoParam.Builder newBuilder() {
            return new GetQuerySegmentInfoParam.Builder();
        }

        public GetQuerySegmentInfoParam.Builder withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetQuerySegmentInfoParam build() {
            return new GetQuerySegmentInfoParam(this);
        }
    }
}
