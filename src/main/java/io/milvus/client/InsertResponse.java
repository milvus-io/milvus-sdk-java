package io.milvus.client;

import java.util.List;

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

    //TODO: iterator


    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("InsertResponse {%s, returned %d vector ids}",
                              response.toString(), this.vectorIds.size());
    }
}
