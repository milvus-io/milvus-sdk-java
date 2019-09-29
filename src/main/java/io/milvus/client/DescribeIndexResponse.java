package io.milvus.client;

import javax.annotation.Nullable;
import java.util.Optional;

public class DescribeIndexResponse {
    private final Response response;
    private final IndexParam indexParam;

    public DescribeIndexResponse(Response response, @Nullable IndexParam indexParam) {
        this.response = response;
        this.indexParam = indexParam;
    }

    public Optional<IndexParam> getIndexParam() {
        return Optional.ofNullable(indexParam);
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("DescribeIndexResponse {%s, %s}", response.toString(),
                              indexParam == null ? "Index param = None" : indexParam.toString());
    }
}
