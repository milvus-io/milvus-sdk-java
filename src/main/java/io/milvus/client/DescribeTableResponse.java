package io.milvus.client;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Contains the returned <code>response</code> and <code>tableSchema</code> for <code>describeTable</code>
 */
public class DescribeTableResponse {
    private final Response response;
    private final TableSchema tableSchema;

    public DescribeTableResponse(Response response, @Nullable TableSchema tableSchema) {
        this.response = response;
        this.tableSchema = tableSchema;
    }

    /**
     * @return an <code>Optional</code> object which may or may not contain a <code>TableSchema</code> object
     * @see Optional
     */
    public Optional<TableSchema> getTableSchema() {
        return Optional.ofNullable(tableSchema);
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("DescribeTableResponse {%s, %s}", response.toString(),
                              tableSchema == null ? "Table schema = None" : tableSchema.toString());
    }
}
