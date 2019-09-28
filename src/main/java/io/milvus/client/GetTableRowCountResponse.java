package io.milvus.client;

public class GetTableRowCountResponse {
    private final Response response;
    private final long tableRowCount;

    public GetTableRowCountResponse(Response response, long tableRowCount) {
        this.response = response;
        this.tableRowCount = tableRowCount;
    }

    public long getTableRowCount() {
        return tableRowCount;
    }

    @Override
    public String toString() {
        return String.format("CountTableResponse {%s, table row count = %d}",
                              response.toString(),
                              tableRowCount);
    }
}
