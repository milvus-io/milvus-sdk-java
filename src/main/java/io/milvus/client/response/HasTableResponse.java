package io.milvus.client.response;

public class HasTableResponse extends Response {
    private final boolean hasTable;

    public HasTableResponse(Status status, String message, boolean hasTable) {
        super(status, message);
        this.hasTable = hasTable;
    }

    public HasTableResponse(Status status, boolean hasTable) {
        super(status);
        this.hasTable = hasTable;
    }

    public boolean hasTable() {
        return hasTable;
    }

    @Override
    public String toString() {
        return String.format("HasTableResponse {code = %s, message = %s, has table = %s}",
                              this.getStatus(), this.getMessage(),
                              hasTable);
    }
}
