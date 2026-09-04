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
import io.milvus.param.Constant;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.mockito.Mockito.verify;

@Tag("integration")
class VectorOrderByTest extends BaseTest {

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
    void testOrderByFieldDefaultsToAsc() {
        OrderByField orderByField = OrderByField.builder()
                .fieldName("price")
                .build();

        Assertions.assertEquals(AggDirection.ASC, orderByField.getDirection());
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}
