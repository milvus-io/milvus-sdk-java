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
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            List<SearchResp.SearchResult> singleResults = new ArrayList<>();
            for (SearchResultsWrapper.IDScore idScore : searchResultsWrapper.getIDScore(i)) {
                singleResults.add(SearchResp.SearchResult.builder()
                        .entity(idScore.getFieldValues())
                        .score(idScore.getScore())
                        .primaryKey(idScore.getPrimaryKey())
                        .id(idScore.getStrID().isEmpty() ? idScore.getLongID() : idScore.getStrID())
                        .build());
            }

            // set highlight and element offset
            SearchResultsWrapper.Position position = searchResultsWrapper.getOffsetByIndex(i);
            long offset = position.getOffset();
            long k = position.getK();
            List<HighlightResult> highlightResults = response.getResults().getHighlightResultsList();
            for (HighlightResult highlightResult : highlightResults) {
                String fieldName = highlightResult.getFieldName();
                List<HighlightData> highlightDatas = highlightResult.getDatasList();
                for (long j = 0; j < k; j++) {
                    HighlightData highlightData = highlightDatas.get((int) (offset + j));
                    List<String> fragments = highlightData.getFragmentsList();
                    List<Float> scores = highlightData.getScoresList();
                    SearchResp.HighlightResult highlightResultObj = SearchResp.HighlightResult.builder()
                            .fieldName(fieldName)
                            .fragments(fragments)
                            .scores(scores)
                            .build();
                    singleResults.get((int) j).addHighlightResult(fieldName, highlightResultObj);
                }
            }

            // set element offset
            if (response.getResults().hasElementIndices()) {
                LongArray elementIndices = response.getResults().getElementIndices();
                for (long j = 0; j < k; j++) {
                    singleResults.get((int) j).setElementOffset(elementIndices.getData((int) (offset + j)));
                }
            }

            searchResults.add(singleResults);
        }
        return searchResults;
    }

    public List<List<AggregationBucket>> getAggregationBuckets(SearchResults response) {
        List<AggregationBucket> buckets = new ArrayList<>();
        for (AggBucket bucket : response.getResults().getAggBucketsList()) {
            buckets.add(convertAggregationBucket(bucket));
        }

        if (buckets.isEmpty()) {
            return new ArrayList<>();
        }

        List<Long> aggTopks = response.getResults().getAggTopksList();
        if (aggTopks.isEmpty()) {
            long numQueries = response.getResults().getNumQueries();
            if (numQueries > 1) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Aggregation buckets were returned without aggTopks for a multi-query search, so results cannot be mapped per query.");
            }
            List<List<AggregationBucket>> aggregationBuckets = new ArrayList<>();
            aggregationBuckets.add(buckets);
            return aggregationBuckets;
        }

        List<List<AggregationBucket>> aggregationBuckets = new ArrayList<>();
        int offset = 0;
        for (Long aggTopk : aggTopks) {
            int size = aggTopk.intValue();
            int end = Math.min(offset + size, buckets.size());
            aggregationBuckets.add(new ArrayList<>(buckets.subList(offset, end)));
            offset = end;
        }
        if (offset < buckets.size()) {
            aggregationBuckets.add(new ArrayList<>(buckets.subList(offset, buckets.size())));
        }
        return aggregationBuckets;
    }

    private AggregationBucket convertAggregationBucket(AggBucket bucket) {
        List<AggregationBucket.KeyEntry> keyEntries = new ArrayList<>();
        for (BucketKeyEntry key : bucket.getKeyList()) {
            keyEntries.add(AggregationBucket.KeyEntry.builder()
                    .fieldId(key.getFieldId())
                    .fieldName(key.getFieldName().isEmpty() ? String.valueOf(key.getFieldId()) : key.getFieldName())
                    .value(getBucketKeyValue(key))
                    .build());
        }

        Map<String, Object> metrics = new HashMap<>();
        for (Map.Entry<String, MetricValue> entry : bucket.getMetricsMap().entrySet()) {
            metrics.put(entry.getKey(), getMetricValue(entry.getValue()));
        }

        List<AggregationHit> hits = new ArrayList<>();
        for (AggHit hit : bucket.getHitsList()) {
            hits.add(convertAggregationHit(hit));
        }

        List<AggregationBucket> subGroups = new ArrayList<>();
        for (AggBucket subBucket : bucket.getSubGroupsList()) {
            subGroups.add(convertAggregationBucket(subBucket));
        }

        return AggregationBucket.builder()
                .key(keyEntries)
                .count(bucket.getCount())
                .metrics(metrics)
                .hits(hits)
                .subGroups(subGroups)
                .build();
    }

    private AggregationHit convertAggregationHit(AggHit hit) {
        AggregationHit.AggregationHitBuilder builder = AggregationHit.builder()
                .score(hit.getScore())
                .id(getAggHitPrimaryKey(hit));
        for (AggHitField field : hit.getFieldsList()) {
            String fieldName = field.getFieldName().isEmpty() ? String.valueOf(field.getFieldId()) : field.getFieldName();
            builder.addField(fieldName, getAggHitFieldValue(field), field.getFieldId());
        }
        return builder.build();
    }

    private Object getAggHitPrimaryKey(AggHit hit) {
        switch (hit.getPkCase()) {
            case INT_PK:
                return hit.getIntPk();
            case STR_PK:
                return hit.getStrPk();
            case PK_NOT_SET:
            default:
                return null;
        }
    }

    private Object getAggHitFieldValue(AggHitField field) {
        switch (field.getValueCase()) {
            case INT_VAL:
                return field.getIntVal();
            case BOOL_VAL:
                return field.getBoolVal();
            case FLOAT_VAL:
                return field.getFloatVal();
            case DOUBLE_VAL:
                return field.getDoubleVal();
            case STRING_VAL:
                return field.getStringVal();
            case BYTES_VAL:
                return field.getBytesVal().toByteArray();
            case VALUE_NOT_SET:
            default:
                return null;
        }
    }

    private Object getBucketKeyValue(BucketKeyEntry key) {
        switch (key.getValueCase()) {
            case INT_VAL:
                return key.getIntVal();
            case STRING_VAL:
                return key.getStringVal();
            case BOOL_VAL:
                return key.getBoolVal();
            case VALUE_NOT_SET:
            default:
                return null;
        }
    }

    private Object getMetricValue(MetricValue value) {
        switch (value.getValueCase()) {
            case INT_VAL:
                return value.getIntVal();
            case DOUBLE_VAL:
                return value.getDoubleVal();
            case STRING_VAL:
                return value.getStringVal();
            case BOOL_VAL:
                return value.getBoolVal();
            case VALUE_NOT_SET:
            default:
                return null;
        }
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
                .fieldNames(Stream.concat(
                                response.getSchema().getFieldsList().stream().map(FieldSchema::getName),
                                response.getSchema().getStructArrayFieldsList().stream().map(StructArrayFieldSchema::getName))
                        .distinct()
                        .collect(Collectors.toList()))
                .vectorFieldNames(Stream.concat(
                                response.getSchema().getFieldsList().stream()
                                        .filter(fieldSchema -> ParamUtils.isVectorDataType(fieldSchema.getDataType()))
                                        .map(FieldSchema::getName),
                                response.getSchema().getStructArrayFieldsList().stream()
                                        .flatMap(structField -> structField.getFieldsList().stream()
                                                .filter(subField -> ParamUtils.isVectorDataType(subField.getElementType()))
                                                .map(subField -> structField.getName() + "[" + subField.getName() + "]")))
                        .distinct()
                        .collect(Collectors.toList()))
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
