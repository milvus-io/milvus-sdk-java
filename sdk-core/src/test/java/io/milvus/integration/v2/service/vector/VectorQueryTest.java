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

package io.milvus.integration.v2.service.vector;

import io.milvus.grpc.*;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorQueryTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorQueryTest.class);

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
    void testQueryExposesServerCost() {
        QueryResults response = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "321")
                        .build())
                .setSessionTs(100L)
                .build();
        when(blockingStub.query(any())).thenReturn(response);

        QueryResp resp = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .limit(10)
                .build());

        Assertions.assertEquals(321L, resp.getCost());
        Assertions.assertEquals(100L, resp.getSessionTs());
    }

    @Test
    void testQueryCostDefaultsToZeroWhenReportValueMissingOrInvalid() {
        QueryResults absent = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .build();
        when(blockingStub.query(any())).thenReturn(absent);
        QueryResp respAbsent = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build());
        Assertions.assertEquals(0L, respAbsent.getCost());

        QueryResults invalid = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).putExtraInfo("report_value", "not-a-number").build())
                .build();
        when(blockingStub.query(any())).thenReturn(invalid);
        QueryResp respInvalid = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build());
        Assertions.assertEquals(0L, respInvalid.getCost());
    }

    @Test
    void testQueryElementLevelExpandsResultsWithOffset() {
        QueryResults response = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(10L).addData(20L).build())
                                .build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).addData(2L).build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(1L).build())
                        .build())
                .build();
        when(blockingStub.query(any())).thenReturn(response);

        QueryResp resp = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .filter("element_filter(clips, $[tag] == \"sports\")")
                .build());

        List<QueryResp.QueryResult> results = resp.getQueryResults();
        Assertions.assertEquals(3, results.size());

        Assertions.assertEquals(10L, results.get(0).getEntity().get("id"));
        Assertions.assertEquals(0L, results.get(0).getElementOffset());

        Assertions.assertEquals(10L, results.get(1).getEntity().get("id"));
        Assertions.assertEquals(2L, results.get(1).getElementOffset());

        Assertions.assertEquals(20L, results.get(2).getEntity().get("id"));
        Assertions.assertEquals(1L, results.get(2).getElementOffset());
    }

    @Test
    void testQueryElementLevelRejectsIndexCountMismatch() {
        // element_indices count (1) does not match the row count (2): must fail, not silently drop
        QueryResults response = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(10L).addData(20L).build())
                                .build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).build())
                        .build())
                .build();
        when(blockingStub.query(any())).thenReturn(response);

        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.query(QueryReq.builder()
                        .collectionName("book")
                        .filter("element_filter(clips, $[tag] == \"sports\")")
                        .build()));
        Assertions.assertEquals(ErrorCode.SERVER_ERROR, ex.getErrorCode());
    }
}
