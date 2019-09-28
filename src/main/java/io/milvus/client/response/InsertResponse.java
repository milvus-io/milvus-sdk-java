package io.milvus.client.response;

import java.util.List;
import java.util.Optional;

public class InsertResponse extends Response {

    private final List<Long> vectorIds;

    public InsertResponse(Status status, String message, List<Long> vectorIds) {
        super(status, message);
        this.vectorIds = vectorIds;
    }

    public InsertResponse(Status status, List<Long> vectorIds) {
        super(status);
        this.vectorIds = vectorIds;
    }

    public List<Long> getVectorIds() {
        return vectorIds;
    }

    //TODO: iterator

    @Override
    public String toString() {
        return String.format("InsertResponse {code = %s, message = %s, returned %d vector ids}",
                             this.getStatus().name(), this.getMessage(), this.vectorIds.size());
    }
}
