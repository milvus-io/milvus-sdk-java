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

import com.google.gson.reflect.TypeToken;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
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
    public static DataType toProtoDataType(io.milvus.v2.common.DataType dt) {
        if (dt == null) {
            return DataType.None;
        }
        try {
            return DataType.valueOf(dt.name());
        } catch (Exception e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Failed to convert data type, error: " + e.getMessage());
        }
    }

    public static io.milvus.v2.common.DataType toSdkDataType(DataType dt) {
        if (dt == null) {
            return io.milvus.v2.common.DataType.None;
        }
        try {
            return io.milvus.v2.common.DataType.valueOf(dt.name());
        } catch (Exception e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Failed to convert data type, error: " + e.getMessage());
        }
    }

    public List<QueryResp.QueryResult> getEntities(QueryResults response) {
        List<QueryResp.QueryResult> entities = new ArrayList<>();
        // count(*) ?
        if (response.getFieldsDataList().stream().anyMatch(fieldData -> fieldData.getFieldName().equals("count(*)"))) {
            Map<String, Object> countField = new HashMap<>();
            long numOfEntities = response.getFieldsDataList().stream().filter(fieldData -> fieldData.getFieldName().equals("count(*)")).map(FieldData::getScalars).collect(Collectors.toList()).get(0).getLongData().getData(0);
            countField.put("count(*)", numOfEntities);

            QueryResp.QueryResult queryResult = QueryResp.QueryResult.builder()
                    .entity(countField)
                    .build();
            entities.add(queryResult);

            return entities;
        }

        // normal query
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(response);
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
                    .primaryKey(idScore.getPrimaryKey())
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
            IndexParam.IndexType indexType = IndexParam.IndexType.None;
            IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
            Map<String, String> properties = new HashMap<>();
            for (KeyValuePair param : params) {
                if (param.getKey().equals(Constant.INDEX_TYPE)) {
                    try {
                        indexType = IndexParam.IndexType.valueOf(param.getValue().toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // if the server has new index type but sdk version is old
                        e.printStackTrace();
                    }
                } else if (param.getKey().equals(Constant.METRIC_TYPE)) {
                    // for scalar index such as Trie/STL_SORT, the param.getValue() is empty, no need to parse it
                    if (!param.getValue().isEmpty()) {
                        try {
                            metricType = IndexParam.MetricType.valueOf(param.getValue());
                        } catch (IllegalArgumentException e) {
                            // if the server has new metric type but sdk version is old
                            e.printStackTrace();
                        }
                    }
                } else if (param.getKey().equals(Constant.MMAP_ENABLED)) {
                    properties.put(param.getKey(), param.getValue()); // just for compatible with old versions
                    extraParams.put(param.getKey(), param.getValue());
                } else if (param.getKey().equals(Constant.PARAMS)) {
                    Map<String, String> tempParams = JsonUtils.fromJson(param.getValue(), new TypeToken<Map<String, String>>() {
                    }.getType());
                    tempParams.remove(Constant.MMAP_ENABLED); // "mmap.enabled" in "params" is not processed by server
                    extraParams.putAll(tempParams);
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
                    .properties(properties)
                    .build();
            descs.add(desc);
        }

        return DescribeIndexResp.builder().indexDescriptions(descs).build();
    }

    public List<DescribeCollectionResp> convertDescCollectionsResp(BatchDescribeCollectionResponse response) {
        List<DescribeCollectionResp> result = new ArrayList<>();
        List<DescribeCollectionResponse> responsesList = response.getResponsesList();
        for (DescribeCollectionResponse collectionResponse : responsesList) {
            DescribeCollectionResp describeCollectionResp = convertDescCollectionResp(collectionResponse);
            result.add(describeCollectionResp);
        }
        return result;
    }

    public DescribeCollectionResp convertDescCollectionResp(DescribeCollectionResponse response) {
        Map<String, String> properties = new HashMap<>();
        response.getPropertiesList().forEach(prop -> properties.put(prop.getKey(), prop.getValue()));

        DescribeCollectionResp describeCollectionResp = DescribeCollectionResp.builder()
                .collectionName(response.getCollectionName())
                .collectionID(response.getCollectionID())
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
                .createUtcTime(response.getCreatedUtcTimestamp())
                .consistencyLevel(io.milvus.v2.common.ConsistencyLevel.valueOf(response.getConsistencyLevel().name().toUpperCase()))
                .shardsNum(response.getShardsNum())
                .properties(properties)
                .build();
        return describeCollectionResp;
    }
}
