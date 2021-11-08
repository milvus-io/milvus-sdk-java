package io.milvus.Response;

import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import lombok.NonNull;

import java.util.List;

public class GetCollStatResponseWrapper {
    private final GetCollectionStatisticsResponse stat;

    public GetCollStatResponseWrapper(@NonNull GetCollectionStatisticsResponse stat) {
        this.stat = stat;
    }

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
