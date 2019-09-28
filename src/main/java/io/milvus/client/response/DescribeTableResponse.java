package io.milvus.client.response;

import io.milvus.client.params.TableSchema;

import javax.annotation.Nullable;
import java.util.Optional;

public class DescribeTableResponse extends Response {
    private final TableSchema tableSchema;

    public DescribeTableResponse(Status status, String message, @Nullable TableSchema tableSchema) {
        super(status, message);
        this.tableSchema = tableSchema;
    }

    public DescribeTableResponse(Status status, @Nullable TableSchema tableSchema) {
        super(status);
        this.tableSchema = tableSchema;
    }

    public Optional<TableSchema> getTableSchema() {
        return Optional.ofNullable(tableSchema);
    }

    @Override
    public String toString() {
        return String.format("DescribeTableResponse {code = %s, message = %s, %s}", this.getStatus(), this.getMessage(),
                             tableSchema == null ? "Table schema = None" : tableSchema.toString());
    }
}
