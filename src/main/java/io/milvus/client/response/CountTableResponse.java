package io.milvus.client.response;

public class CountTableResponse extends Response {
    private final long tableRowCount;

    public CountTableResponse(Status status, String message, long tableRowCount) {
        super(status, message);
        this.tableRowCount = tableRowCount;
    }

    public CountTableResponse(Status status, long tableRowCount) {
        super(status);
        this.tableRowCount = tableRowCount;
    }

    public long getTableRowCount() {
        return tableRowCount;
    }

    @Override
    public String toString() {
        return String.format("CountTableResponse {code = %s, message = %s, table row count = %d}",
                              this.getStatus(), this.getMessage(),
                              tableRowCount);
    }
}
