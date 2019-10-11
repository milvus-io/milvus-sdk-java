package io.milvus.client;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Contains the returned <code>response</code> and <code>index</code> for <code>describeIndex</code>
 */
public class DescribeIndexResponse {
    private final Response response;
    private final Index index;

    public DescribeIndexResponse(Response response, @Nullable Index index) {
        this.response = response;
        this.index = index;
    }

    /**
     * @return an <code>Optional</code> object which may or may not contain an <code>Index</code> object
     * @see Optional
     */
    public Optional<Index> getIndex() {
        return Optional.ofNullable(index);
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("DescribeIndexResponse {%s, %s}", response.toString(),
                             index == null ? "Index = Null" : index.toString());
    }
}
