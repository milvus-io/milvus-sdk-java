package io.milvus.client;

/**
 * Contains the returned <code>response</code> and <code>tableRowCount</code> for <code>getTableRowCount</code>
 */
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

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("CountTableResponse {%s, table row count = %d}",
                              response.toString(),
                              tableRowCount);
    }
}
