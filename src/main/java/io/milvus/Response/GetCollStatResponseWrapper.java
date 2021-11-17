package io.milvus.Response;

import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import lombok.NonNull;

import java.util.List;

/**
 * Util class to wrap response of <code>getCollectionStatistics</code> interface.
 */
public class GetCollStatResponseWrapper {
    private final GetCollectionStatisticsResponse stat;

    public GetCollStatResponseWrapper(@NonNull GetCollectionStatisticsResponse stat) {
        this.stat = stat;
    }

    /**
     * Get row count of this field.
     * Throw {@link NumberFormatException} if the row count is not a number.
     *
     * @return <code>int</code> dimension of the vector field
     */
    public long GetRowCount() throws NumberFormatException {
        List<KeyValuePair> stats = stat.getStatsList();
        for (KeyValuePair kv : stats) {
            if (kv.getKey().compareTo("row_count") == 0) {
                return Long.parseLong(kv.getValue());
            }
        }

        return 0;
    }
}
