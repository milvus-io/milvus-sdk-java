package io.milvus.client.response;

import java.util.List;

public class ShowTablesResponse extends Response {
    private final List<String> tableNames;

    public ShowTablesResponse(Status status, String message, List<String> tableNames) {
        super(status, message);
        this.tableNames = tableNames;
    }

    public ShowTablesResponse(Status status, List<String> tableNames) {
        super(status);
        this.tableNames = tableNames;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    @Override
    public String toString() {
        return String.format("ShowTablesResponse {code = %s, message = %s, table names = %s}",
                              this.getStatus(), this.getMessage(),
                              tableNames.toString());
    }
}
