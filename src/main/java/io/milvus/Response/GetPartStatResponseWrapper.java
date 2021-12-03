package io.milvus.Response;

import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import lombok.NonNull;

import java.util.List;

/**
 * Utility class to wrap response of <code>getPartitionStatistics</code> interface.
 */
public class GetPartStatResponseWrapper {
    private final GetPartitionStatisticsResponse stat;

    public GetPartStatResponseWrapper(@NonNull GetPartitionStatisticsResponse stat) {
        this.stat = stat;
    }

    /**
     * Gets the row count of a field.
     * Throw {@link NumberFormatException} if the row count is not a number.
     *
     * @return <code>int</code> dimension of the vector field
     */
    public long getRowCount() throws NumberFormatException {
        List<KeyValuePair> stats = stat.getStatsList();
        for (KeyValuePair kv : stats) {
            if (kv.getKey().compareTo("row_count") == 0) {
                return Long.parseLong(kv.getValue());
            }
        }

        return 0;
    }
}
