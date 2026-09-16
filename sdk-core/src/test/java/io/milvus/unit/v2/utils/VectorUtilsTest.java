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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.VectorUtils;

import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.TemplateValue;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class VectorUtilsTest {
    private final CollectionTsCache timestampCache = CollectionTsCache.getInstance();

    @BeforeEach
    @AfterEach
    void clearTimestampCache() {
        timestampCache.clear();
    }

    @Test
    void deduceAndCreateTemplateValueHandlesAllSupportedTypes() {
        Assertions.assertTrue(VectorUtils.deduceAndCreateTemplateValue(Boolean.TRUE).getBoolVal());
        assertEquals(5L, VectorUtils.deduceAndCreateTemplateValue(5).getInt64Val());
        assertEquals(5L, VectorUtils.deduceAndCreateTemplateValue(5L).getInt64Val());
        assertEquals(1.5d, VectorUtils.deduceAndCreateTemplateValue(1.5d).getFloatVal());
        assertEquals("str", VectorUtils.deduceAndCreateTemplateValue("str").getStringVal());
        assertEquals(2, VectorUtils.deduceAndCreateTemplateValue(new byte[]{1, 2}).getBytesVal().size());
        assertEquals(2, VectorUtils.deduceAndCreateTemplateValue(Arrays.asList(1L, 2L))
                .getArrayVal().getLongData().getDataCount());
    }

    @Test
    void deduceAndCreateTemplateValueRejectsUnsupportedType() {
        assertThrows(MilvusClientException.class, () -> VectorUtils.deduceAndCreateTemplateValue(1.5f));
        assertThrows(MilvusClientException.class, () -> VectorUtils.deduceAndCreateTemplateValue(new Object()));
    }

    @Test
    void getExprByIdBuildsFilterExpression() {
        VectorUtils vectorUtils = new VectorUtils();
        assertEquals("id in [1,2]", vectorUtils.getExprById("id", Arrays.asList(1L, 2L)));
        assertEquals("id in [\"a\",\"b\"]", vectorUtils.getExprById("id", Arrays.asList("a", "b")));
    }

    @Test
    void convertAnnSearchParamBuildsSearchRequestAndRejectsInvalidInput() {
        BaseVector vector = new FloatVec(Collections.singletonList(1.0f));
        AnnSearchReq request = AnnSearchReq.builder()
                .vectorFieldName("vec")
                .vectors(Collections.singletonList(vector))
                .topK(10)
                .metricType(io.milvus.v2.common.IndexParam.MetricType.COSINE)
                .filter("id > 0")
                .build();

        SearchRequest grpcRequest = VectorUtils.convertAnnSearchParam(request, ConsistencyLevel.STRONG);
        assertEquals(1, grpcRequest.getNq());
        assertEquals("id > 0", grpcRequest.getDsl());
        assertEquals(io.milvus.grpc.DslType.BoolExprV1, grpcRequest.getDslType());
        assertTrue(grpcRequest.getPlaceholderGroup().size() > 0);

        assertThrows(MilvusClientException.class,
                () -> VectorUtils.convertAnnSearchParam(AnnSearchReq.builder()
                        .vectorFieldName("vec")
                        .vectors(Collections.emptyList())
                        .build(), ConsistencyLevel.STRONG));
    }

    @Test
    void convertAnnSearchParamRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> VectorUtils.convertAnnSearchParam(null, ConsistencyLevel.STRONG));
    }

    @Test
    void conversionUsesOnlyItsConfiguredEndpoint() {
        timestampCache.set("host-a:19530", "db", "coll", 100L);
        timestampCache.set("host-b:19530", "db", "coll", 200L);
        timestampCache.set("host-c:19530", "other-db", "coll", 300L);

        VectorUtils endpointlessUtils = new VectorUtils();
        QueryRequest sessionRequest = endpointlessUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());
        QueryRequest defaultConsistencyRequest = endpointlessUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .build());

        VectorUtils endpointUtils = new VectorUtils();
        endpointUtils.setEndpoint("host-a:19530");
        endpointUtils.setCurrentDbName("db");
        QueryRequest endpointRequest = endpointUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .collectionName("coll")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());

        assertEquals(1L, sessionRequest.getGuaranteeTimestamp());
        assertEquals(1L, defaultConsistencyRequest.getGuaranteeTimestamp());
        assertEquals(100L, endpointRequest.getGuaranteeTimestamp());
    }

    @Test
    void convertsFunctionWithNullParamsAsEmptyMap() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("reranker")
                .functionType(FunctionType.RERANK)
                .params(null)
                .build();

        SearchRequest request = new VectorUtils().ConvertToGrpcSearchRequest(SearchReq.builder()
                .collectionName("coll")
                .ids(Collections.singletonList(1L))
                .limit(1)
                .functionScore(FunctionScore.builder().addFunction(function).build())
                .build());

        assertEquals(0, request.getFunctionScore().getFunctions(0).getParamsCount());
    }
}
