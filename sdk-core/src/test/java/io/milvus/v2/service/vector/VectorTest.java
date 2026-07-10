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

package io.milvus.v2.service.vector;

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VectorTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorTest.class);

    @Test
    void testInsert() {

        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject vector = new JsonObject();
            List<Float> vectorList = new ArrayList<>();
            vectorList.add(1.0f);
            vectorList.add(2.0f);
            vector.add("vector", JsonUtils.toJsonTree(vectorList));
            vector.addProperty("id", (long) i);
            data.add(vector);
        }

        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(data)
                .build();
        InsertResp statusR = client_v2.insert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testUpsert() {

        JsonObject jsonObject = new JsonObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.add("vector", JsonUtils.toJsonTree(vectorList));
        jsonObject.addProperty("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(jsonObject))
                .build();

        UpsertResp statusR = client_v2.upsert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testUpsertWithFieldOps() {
        JsonObject jsonObject = new JsonObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.add("vector", JsonUtils.toJsonTree(vectorList));
        jsonObject.addProperty("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(jsonObject))
                .fieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("vector")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                        .build()))
                .build();

        client_v2.upsert(request);

        ArgumentCaptor<UpsertRequest> captor = ArgumentCaptor.forClass(UpsertRequest.class);
        verify(blockingStub).upsert(captor.capture());
        UpsertRequest rpcRequest = captor.getValue();
        Assertions.assertTrue(rpcRequest.getPartialUpdate());
        Assertions.assertEquals(1, rpcRequest.getFieldOpsCount());
        Assertions.assertEquals("vector", rpcRequest.getFieldOps(0).getFieldName());
        Assertions.assertEquals(io.milvus.grpc.FieldPartialUpdateOp.OpType.ARRAY_APPEND, rpcRequest.getFieldOps(0).getOp());
    }

    @Test
    void testQuery() {
        QueryReq req = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(0))
                .limit(10)
                //.outputFields(Collections.singletonList("count(*)"))
                .build();
        QueryResp resultsR = client_v2.query(req);

        logger.info(resultsR.toString());
    }

    @Test
    void testSearch() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .offset(0L)
                .build();
        SearchResp statusR = client_v2.search(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(123L, statusR.getCost());
        Assertions.assertEquals(456L, statusR.getScannedRemoteBytes());
        Assertions.assertEquals(789L, statusR.getScannedTotalBytes());
        Assertions.assertEquals(0.5f, statusR.getCacheHitRatio());
    }

    @Test
    void testSearchWithTemplateExpression() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);

        Map<String, Map<String, Object>> expressionTemplateValues = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        params.put("max", 10);
        expressionTemplateValues.put("id < {max}", params);

        List<Object> list = Arrays.asList(1, 2, 3);
        Map<String, Object> params2 = new HashMap<>();
        params2.put("list", list);
        expressionTemplateValues.put("id in {list}", params2);

        expressionTemplateValues.forEach((key, value) -> {
            SearchReq request = SearchReq.builder()
                    .collectionName("test")
                    .data(Collections.singletonList(new FloatVec(vectorList)))
                    .limit(10)
                    .offset(0L)
                    .filter(key)
                    .filterTemplateValues(value)
                    .build();
            SearchResp statusR = client_v2.search(request);
            logger.info(statusR.toString());
            System.out.println(statusR);
        });
    }

    @Test
    void testDelete() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testDeleteById() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList("0"))
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testGet() {
        GetReq request = GetReq.builder()
                .collectionName("test2")
                .ids(Collections.singletonList("447198483337881033"))
                .build();
        GetResp statusR = client_v2.get(request);
        logger.info(statusR.toString());
    }

    @Test
    void testQueryOrderByFieldsSerialization() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .orderByFields(Arrays.asList(
                        OrderByField.builder().fieldName("price").build(),
                        OrderByField.builder().fieldName("rating").direction(AggDirection.DESC).build()))
                .build();

        client_v2.query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("price:asc,rating:desc", getParam(captor.getValue().getQueryParamsList(), Constant.ORDER_BY_FIELDS));
    }

    @Test
    void testSearchOrderByFieldsSerialization() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .orderByFields(Arrays.asList(
                        OrderByField.builder().fieldName("price").direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName("rating").direction(AggDirection.DESC).build()))
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("price:asc,rating:desc", getParam(captor.getValue().getSearchParamsList(), Constant.ORDER_BY_FIELDS));
    }

    @Test
    void testSessionSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionQueryPassesClusterId() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();

        client_v2.session("cluster-a").query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionGetPassesClusterId() {
        GetReq request = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .build();

        client_v2.session("cluster-a").get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionHybridSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();

        client_v2.session("cluster-a").hybridSearch(request);

        ArgumentCaptor<HybridSearchRequest> captor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(blockingStub).hybridSearch(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getRankParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionRejectsClosedAndConflictingClusterId() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        MilvusClientException conflictException = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.session("cluster-a").search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, conflictException.getErrorCode());

        io.milvus.v2.client.MilvusClientV2Session session = client_v2.session("cluster-a");
        session.close();
        SearchReq closedRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        MilvusClientException closedException = Assertions.assertThrows(MilvusClientException.class,
                () -> session.search(closedRequest));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, closedException.getErrorCode());
    }

    @Test
    void testOrderByFieldDefaultsToAsc() {
        OrderByField orderByField = OrderByField.builder()
                .fieldName("price")
                .build();

        Assertions.assertEquals(AggDirection.ASC, orderByField.getDirection());
    }

    @Test
    void testOrderByFieldRejectsComma() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> OrderByField.builder().fieldName("metadata[\"a,b\"]").build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testOrderByFieldRejectsColon() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> OrderByField.builder().fieldName("metadata[\"a:b\"]").build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testSearchAggregationSerialization() {
        SearchAggregation requestAggregation = buildAggregation();
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(requestAggregation)
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        SearchAggregationSpec aggregation = captor.getValue().getSearchAggregation();
        Assertions.assertEquals(Arrays.asList("category", "region"), aggregation.getFieldsList());
        Assertions.assertEquals(5, aggregation.getSize());
        Assertions.assertEquals("count", aggregation.getMetricsOrThrow("doc_count").getOp());
        Assertions.assertEquals("*", aggregation.getMetricsOrThrow("doc_count").getFieldName());
        Assertions.assertEquals("avg", aggregation.getMetricsOrThrow("avg_score").getOp());
        Assertions.assertEquals("score", aggregation.getMetricsOrThrow("avg_score").getFieldName());
        Assertions.assertEquals("doc_count", aggregation.getOrder(0).getKey());
        Assertions.assertEquals("desc", aggregation.getOrder(0).getDirection());
        Assertions.assertEquals(2, aggregation.getTopHits().getSize());
        Assertions.assertEquals("_score", aggregation.getTopHits().getSort(0).getFieldName());
        Assertions.assertEquals("desc", aggregation.getTopHits().getSort(0).getDirection());
        Assertions.assertEquals(3, aggregation.getSubAggregation().getSize());
        Assertions.assertEquals(Collections.singletonList("brand"), aggregation.getSubAggregation().getFieldsList());
    }

    @Test
    void testSearchAggregationRejectsGroupBy() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .groupByFieldName("category")
                .searchAggregation(buildAggregation())
                .build();

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testSessionSearchPreservesAggregationAndClusterId() {
        SearchAggregation requestAggregation = buildAggregation();
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(requestAggregation)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(Arrays.asList("category", "region"), captor.getValue().getSearchAggregation().getFieldsList());
        Assertions.assertNull(request.getClusterId());
        Assertions.assertSame(requestAggregation, request.getSearchAggregation());
    }

    @Test
    void testSearchWithoutAggregationKeepsAggregationBucketsEmpty() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .setNumQueries(1)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).build()).build())
                        .addScores(0.9f)
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        SearchResp searchResp = client_v2.search(request);

        Assertions.assertTrue(searchResp.getAggregationBuckets().isEmpty());
    }

    @Test
    void testSearchAggregationWithoutAggTopksFailsForMultiQuery() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .addTopks(1)
                        .setNumQueries(2)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).addData(11L).build()).build())
                        .addScores(0.9f)
                        .addScores(0.8f)
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(101).setFieldName("category").setStringVal("books").build())
                                .setCount(7)
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(102).setFieldName("category").setStringVal("games").build())
                                .setCount(2)
                                .build())
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Arrays.asList(
                        new FloatVec(Arrays.asList(1.0f, 2.0f)),
                        new FloatVec(Arrays.asList(3.0f, 4.0f))))
                .limit(10)
                .searchAggregation(buildAggregation())
                .build();
        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, ex.getErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("without aggTopks"));
        Assertions.assertTrue(ex.getMessage().contains("multi-query search"));
    }

    @Test
    void testSearchAggregationResponseParsing() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "123")
                        .putExtraInfo("scanned_remote_bytes", "456")
                        .putExtraInfo("scanned_total_bytes", "789")
                        .putExtraInfo("cache_hit_ratio", "0.5")
                        .build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .setNumQueries(1)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).build()).build())
                        .addScores(0.9f)
                        .addFieldsData(FieldData.newBuilder()
                                .setFieldName("label")
                                .setType(DataType.VarChar)
                                .setScalars(ScalarField.newBuilder()
                                        .setStringData(StringArray.newBuilder().addData("doc-1").build())
                                        .build())
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(101).setFieldName("category").setStringVal("books").build())
                                .setCount(7)
                                .putMetrics("doc_count", MetricValue.newBuilder().setIntVal(7L).build())
                                .putMetrics("avg_score", MetricValue.newBuilder().setDoubleVal(0.88d).build())
                                .addHits(AggHit.newBuilder()
                                        .setIntPk(10L)
                                        .setScore(0.91f)
                                        .addFields(AggHitField.newBuilder().setFieldId(201).setFieldName("title").setStringVal("Book A").build())
                                        .addFields(AggHitField.newBuilder().setFieldId(202).setFieldName("payload").setBytesVal(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3})).build())
                                        .build())
                                .addSubGroups(AggBucket.newBuilder()
                                        .addKey(BucketKeyEntry.newBuilder().setFieldId(102).setFieldName("brand").setStringVal("acme").build())
                                        .setCount(3)
                                        .build())
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(103).setFieldName("category").setStringVal("games").build())
                                .setCount(2)
                                .build())
                        .addAggTopks(1)
                        .addAggTopks(1)
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(buildAggregation())
                .build();
        SearchResp searchResp = client_v2.search(request);

        Assertions.assertEquals(1, searchResp.getSearchResults().size());
        Assertions.assertEquals(1, searchResp.getSearchResults().get(0).size());
        Assertions.assertEquals(10L, searchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(0.9f, searchResp.getSearchResults().get(0).get(0).getScore());
        Assertions.assertEquals(2, searchResp.getAggregationBuckets().size());
        Assertions.assertEquals(1, searchResp.getAggregationBuckets().get(0).size());
        Assertions.assertEquals(1, searchResp.getAggregationBuckets().get(1).size());
        AggregationBucket bucket = searchResp.getAggregationBuckets().get(0).get(0);
        Assertions.assertEquals(7L, bucket.getCount());
        Assertions.assertEquals("books", bucket.getKey().get(0).getValue());
        Assertions.assertEquals(7L, bucket.getMetrics().get("doc_count"));
        Assertions.assertEquals(0.88d, bucket.getMetrics().get("avg_score"));
        Assertions.assertEquals(1, bucket.getHits().size());
        AggregationHit hit = bucket.getHits().get(0);
        Assertions.assertEquals(10L, hit.getId());
        Assertions.assertEquals(0.91f, hit.getScore());
        Assertions.assertEquals("Book A", hit.getFields().get("title"));
        Assertions.assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) hit.getFields().get("payload"));
        Assertions.assertEquals(201L, hit.getFieldIds().get("title"));
        Assertions.assertEquals(202L, hit.getFieldIds().get("payload"));
        Assertions.assertEquals(1, bucket.getSubGroups().size());
        Assertions.assertEquals("acme", bucket.getSubGroups().get(0).getKey().get(0).getValue());
        Assertions.assertEquals("books", searchResp.getAggregationBuckets().get(0).get(0).getKey().get(0).getValue());
        Assertions.assertEquals("games", searchResp.getAggregationBuckets().get(1).get(0).getKey().get(0).getValue());
    }

    private SearchAggregation buildAggregation() {
        return SearchAggregation.builder()
                .fields(Arrays.asList("category", "region"))
                .size(5)
                .addMetric("doc_count", MetricSpec.builder().op(MetricOps.COUNT).fieldName("*").build())
                .addMetric("avg_score", MetricSpec.builder().op(MetricOps.AVG).fieldName("score").build())
                .addOrder(OrderSpec.builder().key("doc_count").direction(AggDirection.DESC).build())
                .topHits(TopHitsSpec.builder()
                        .size(2)
                        .addSort(SortSpec.builder().fieldName("_score").direction(AggDirection.DESC).build())
                        .build())
                .subAggregation(SearchAggregation.builder()
                        .addField("brand")
                        .size(3)
                        .build())
                .build();
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}