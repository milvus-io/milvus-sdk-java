package io.milvus.response;

import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.Constant;
import lombok.NonNull;

import java.util.List;

/**
 * Utility class to wrap response of <code>getCollectionStatistics</code> interface.
 */
public class GetCollStatResponseWrapper {
    private final GetCollectionStatisticsResponse stat;

    public GetCollStatResponseWrapper(@NonNull GetCollectionStatisticsResponse stat) {
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
            if (kv.getKey().compareTo(Constant.ROW_COUNT) == 0) {
                return Long.parseLong(kv.getValue());
            }
        }

        return 0;
    }
}
