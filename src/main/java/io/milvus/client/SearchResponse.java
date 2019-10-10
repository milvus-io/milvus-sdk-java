package io.milvus.client;

import java.util.ArrayList;
import java.util.List;

public class SearchResponse {

    private final Response response;

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

    public SearchResponse(Response response, List<List<QueryResult>> queryResultsList) {
        this.response = response;
        this.queryResultsList = queryResultsList;
    }

    public List<List<QueryResult>> getQueryResultsList() {
        return queryResultsList;
    }

    public List<List<Long>> getResultIdsList() {
        List<List<Long>> resultIdsList = new ArrayList<>();
        for (List<QueryResult> queryResults : queryResultsList) {
            List<Long> resultIds = new ArrayList<>();
            for (QueryResult queryResult : queryResults) {
                resultIds.add(queryResult.vectorId);
            }
            resultIdsList.add(resultIds);
        }
        return resultIdsList;
    }

    public List<List<Double>> getResultDistancesList() {
        List<List<Double>> resultDistancesList = new ArrayList<>();
        for (List<QueryResult> queryResults : queryResultsList) {
            List<Double> resultDistances = new ArrayList<>();
            for (QueryResult queryResult : queryResults) {
                resultDistances.add(queryResult.distance);
            }
            resultDistancesList.add(resultDistances);
        }
        return resultDistancesList;
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return String.format("SearchResponse {%s, returned results for %d queries}",
                              response.toString(), this.queryResultsList.size());
    }
}
