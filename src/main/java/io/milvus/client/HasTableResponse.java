package io.milvus.client;

public class HasTableResponse {
    private final Response response;
    private final boolean hasTable;

    public HasTableResponse(Response response, boolean hasTable) {
        this.response = response;
        this.hasTable = hasTable;
    }

    public boolean hasTable() {
        return hasTable;
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("HasTableResponse {%s, has table = %s}",
                              response.toString(),
                              hasTable);
    }
}
