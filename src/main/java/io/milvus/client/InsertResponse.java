package io.milvus.client;

import java.util.List;

/**
 * Contains the returned <code>response</code> and <code>vectorIds</code> for <code>insert</code>
 */
public class InsertResponse {
    private final Response response;
    private final List<Long> vectorIds;

    public InsertResponse(Response response, List<Long> vectorIds) {
        this.response = response;
        this.vectorIds = vectorIds;
    }

    public List<Long> getVectorIds() {
        return vectorIds;
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("InsertResponse {%s, returned %d vector ids}",
                              response.toString(), this.vectorIds.size());
    }
}
