package io.milvus.v2.utils;

import io.milvus.grpc.*;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConvertUtils {
    public List<QueryResp.QueryResult> getEntities(QueryResults response) {
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(response);
        List<QueryResp.QueryResult> entities = new ArrayList<>();

        if(response.getFieldsDataList().stream().anyMatch(fieldData -> fieldData.getFieldName().equals("count(*)"))){
            Map<String, Object> countField = new HashMap<>();
            long numOfEntities = response.getFieldsDataList().stream().filter(fieldData -> fieldData.getFieldName().equals("count(*)")).map(FieldData::getScalars).collect(Collectors.toList()).get(0).getLongData().getData(0);
            countField.put("count(*)", numOfEntities);

            QueryResp.QueryResult queryResult = QueryResp.QueryResult.builder()
                    .entity(countField)
                    .build();
            entities.add(queryResult);

            return entities;
        }
        queryResultsWrapper.getRowRecords().forEach(rowRecord -> {
            QueryResp.QueryResult queryResult = QueryResp.QueryResult.builder()
                    .entity(rowRecord.getFieldValues())
                    .build();
            entities.add(queryResult);
        });
        return entities;
    }

    public List<List<SearchResp.SearchResult>> getEntities(SearchResults response) {
        SearchResultsWrapper searchResultsWrapper = new SearchResultsWrapper(response.getResults());
        long numQueries = response.getResults().getNumQueries();
        List<List<SearchResp.SearchResult>> searchResults = new ArrayList<>();
        for (int i = 0; i < numQueries; i++) {
            searchResults.add(searchResultsWrapper.getIDScore(i).stream().map(idScore -> SearchResp.SearchResult.builder()
                    .entity(idScore.getFieldValues())
                    .distance(idScore.getScore())
                    .id(idScore.getStrID().isEmpty() ? idScore.getLongID() : idScore.getStrID())
                    .build()).collect(Collectors.toList()));
        }
        return searchResults;
    }

    public DescribeIndexResp convertToDescribeIndexResp(IndexDescription response) {
        DescribeIndexResp describeIndexResp = DescribeIndexResp.builder()
                .indexName(response.getIndexName())
                .fieldName(response.getFieldName())
                .build();
        List<KeyValuePair> params = response.getParamsList();
        for(KeyValuePair param : params) {
            if (param.getKey().equals("index_type")) {
                describeIndexResp.setIndexType(param.getValue());
            }else if (param.getKey().equals("metric_type")) {
                describeIndexResp.setMetricType(param.getValue());
            }
        }
        return describeIndexResp;
    }
}
