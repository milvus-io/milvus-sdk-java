package io.milvus.client.response;

import io.milvus.client.params.IndexParam;

import javax.annotation.Nullable;
import java.util.Optional;

public class DescribeIndexResponse extends Response {
    private final IndexParam indexParam;

    public DescribeIndexResponse(Status status, String message, @Nullable IndexParam indexParam) {
        super(status, message);
        this.indexParam = indexParam;
    }

    public DescribeIndexResponse(Status status, @Nullable IndexParam indexParam) {
        super(status);
        this.indexParam = indexParam;
    }

    public Optional<IndexParam> getIndexParam() {
        return Optional.ofNullable(indexParam);
    }

    @Override
    public String toString() {
        return String.format("DescribeIndexResponse {code = %s, message = %s, %s}", this.getStatus(), this.getMessage(),
                              indexParam == null ? "Index param = None" : indexParam.toString());
    }
}
