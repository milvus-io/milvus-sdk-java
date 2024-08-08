/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.utils;

import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
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
                    .score(idScore.getScore())
                    .id(idScore.getStrID().isEmpty() ? idScore.getLongID() : idScore.getStrID())
                    .build()).collect(Collectors.toList()));
        }
        return searchResults;
    }

    public DescribeIndexResp convertToDescribeIndexResp(List<IndexDescription> response) {
        List<DescribeIndexResp.IndexDesc> descs = new ArrayList<>();
        for (IndexDescription description : response) {
            Map<String, String> extraParams = new HashMap<>();
            List<KeyValuePair> params = description.getParamsList();
            IndexParam.IndexType indexType = IndexParam.IndexType.NONE;
            IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
            for(KeyValuePair param : params) {
                if (param.getKey().equals(Constant.INDEX_TYPE)) {
                    // may throw IllegalArgumentException
                    indexType = IndexParam.IndexType.valueOf(param.getValue().toUpperCase());
                } else if (param.getKey().equals(Constant.METRIC_TYPE)) {
                    // may throw IllegalArgumentException
                    metricType = IndexParam.MetricType.valueOf(param.getValue());
                } else {
                    extraParams.put(param.getKey(), param.getValue());
                }
            }

            DescribeIndexResp.IndexDesc desc = DescribeIndexResp.IndexDesc.builder()
                    .indexName(description.getIndexName())
                    .fieldName(description.getFieldName())
                    .id(description.getIndexID())
                    .indexType(indexType)
                    .metricType(metricType)
                    .totalRows(description.getTotalRows())
                    .indexedRows(description.getIndexedRows())
                    .pendingIndexRows(description.getPendingIndexRows())
                    .indexState(IndexBuildState.valueOf(description.getState().name()))
                    .indexFailedReason(description.getIndexStateFailReason())
                    .extraParams(extraParams)
                    .build();
            descs.add(desc);
        }

        return DescribeIndexResp.builder().indexDescriptions(descs).build();
    }

    public DescribeCollectionResp convertDescCollectionResp(DescribeCollectionResponse response) {
        Map<String, String> properties = new HashMap<>();
        response.getPropertiesList().forEach(prop->properties.put(prop.getKey(), prop.getValue()));

        DescribeCollectionResp describeCollectionResp = DescribeCollectionResp.builder()
                .collectionName(response.getCollectionName())
                .databaseName(response.getDbName())
                .description(response.getSchema().getDescription())
                .numOfPartitions(response.getNumPartitions())
                .collectionSchema(SchemaUtils.convertFromGrpcCollectionSchema(response.getSchema()))
                .autoID(response.getSchema().getFieldsList().stream().anyMatch(FieldSchema::getAutoID))
                .enableDynamicField(response.getSchema().getEnableDynamicField())
                .fieldNames(response.getSchema().getFieldsList().stream().map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()))
                .vectorFieldNames(response.getSchema().getFieldsList().stream().filter(fieldSchema -> ParamUtils.isVectorDataType(fieldSchema.getDataType())).map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()))
                .primaryFieldName(response.getSchema().getFieldsList().stream().filter(FieldSchema::getIsPrimaryKey).map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()).get(0))
                .createTime(response.getCreatedTimestamp())
                .consistencyLevel(io.milvus.v2.common.ConsistencyLevel.valueOf(response.getConsistencyLevel().name().toUpperCase()))
                .properties(properties)
                .build();
        return describeCollectionResp;
    }
}
