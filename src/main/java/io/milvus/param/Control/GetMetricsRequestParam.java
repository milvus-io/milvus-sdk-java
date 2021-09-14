package io.milvus.param.Control;

/**
 * @author:weilongzhao
 * @time:2021/9/4 23:15
 */
public class GetMetricsRequestParam {
    private final String request;

    public String getRequest() {
        return request;
    }

    public GetMetricsRequestParam(GetMetricsRequestParam.Builder builder) {
        this.request = builder.request;
    }

    public static final class Builder {
        private String request;

        private Builder() {
        }

        public static GetMetricsRequestParam.Builder newBuilder() {
            return new GetMetricsRequestParam.Builder();
        }

        public GetMetricsRequestParam.Builder withCollectionName(String request) {
            this.request = request;
            return this;
        }

        public GetMetricsRequestParam build() {
            return new GetMetricsRequestParam(this);
        }
    }
}
