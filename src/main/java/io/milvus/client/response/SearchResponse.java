package io.milvus.client.response;

import java.util.List;
import java.util.Optional;

public class SearchResponse extends Response {

    public static class QueryResult {
        private final long vectorId;
        private final double distance;

        public QueryResult(long vectorId, double distance) {
            this.vectorId = vectorId;
            this.distance = distance;
        }

        public long getVectorId() {
            return vectorId;
        }

        public double getDistance() {
            return distance;
        }
    }

    private final List<List<QueryResult>> queryResultsList;

    public SearchResponse(Status status, String message, List<List<QueryResult>> queryResultsList) {
        super(status, message);
        this.queryResultsList = queryResultsList;
    }

    public SearchResponse(Status status, List<List<QueryResult>> queryResultsList) {
        super(status);
        this.queryResultsList = queryResultsList;
    }

    public List<List<QueryResult>> getQueryResultsList() {
        return queryResultsList;
    }

    //TODO: iterator

    @Override
    public String toString() {
        return String.format("SearchResponse {code = %s, message = %s, returned results for %d queries}",
                this.getStatus().name(), this.getMessage(), this.queryResultsList.size());
    }
}
