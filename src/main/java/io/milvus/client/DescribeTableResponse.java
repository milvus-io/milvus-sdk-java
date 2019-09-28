package io.milvus.client;

import javax.annotation.Nullable;
import java.util.Optional;

public class DescribeTableResponse {
    private final Response response;
    private final TableSchema tableSchema;

    public DescribeTableResponse(Response response, @Nullable TableSchema tableSchema) {
        this.response = response;
        this.tableSchema = tableSchema;
    }

    public Optional<TableSchema> getTableSchema() {
        return Optional.ofNullable(tableSchema);
    }

    @Override
    public String toString() {
        return String.format("DescribeTableResponse {%s, %s}", response.toString(),
                              tableSchema == null ? "Table schema = None" : tableSchema.toString());
    }
}
