package io.milvus.client;

import java.util.List;

/**
 * Contains the returned <code>response</code> and <code>tableNames</code> for <code>showTables</code>
 */
public class ShowTablesResponse {
    private final Response response;
    private final List<String> tableNames;

    public ShowTablesResponse(Response response, List<String> tableNames) {
        this.response = response;
        this.tableNames = tableNames;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("ShowTablesResponse {%s, table names = %s}",
                              response,
                              tableNames.toString());
    }
}
