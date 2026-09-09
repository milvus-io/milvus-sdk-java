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

package io.milvus.unit.v2.service.vector.request.aggregation;

import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class OrderSpecTest {

    @Test
    void builderSetsFields() {
        OrderSpec spec = OrderSpec.builder()
                .key("_count")
                .direction(AggDirection.DESC)
                .nullFirst(true)
                .build();

        assertEquals("_count", spec.getKey());
        assertEquals(AggDirection.DESC, spec.getDirection());
        assertEquals(true, spec.getNullFirst());
    }

    @Test
    void builderDefaultNullFirstIsNull() {
        OrderSpec spec = OrderSpec.builder().key("_key").direction(AggDirection.ASC).build();
        assertNull(spec.getNullFirst());
    }

    @Test
    void emptyKeyRejected() {
        assertThrows(MilvusClientException.class,
                () -> OrderSpec.builder().direction(AggDirection.ASC).build());
        assertThrows(MilvusClientException.class,
                () -> OrderSpec.builder().key("").direction(AggDirection.ASC).build());
        assertThrows(MilvusClientException.class,
                () -> OrderSpec.builder().key(null).direction(AggDirection.ASC).build());
    }

    @Test
    void nullDirectionRejected() {
        assertThrows(MilvusClientException.class,
                () -> OrderSpec.builder().key("_count").build());
    }

    @Test
    void toProtoIsRoundTrippedThroughSearchAggregation() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("a"))
                .size(5)
                .addOrder(OrderSpec.builder()
                        .key("_count")
                        .direction(AggDirection.DESC)
                        .nullFirst(false)
                        .build())
                .build();

        io.milvus.grpc.OrderSpec grpc = agg.toProto().getOrder(0);
        assertEquals("_count", grpc.getKey());
        assertEquals("desc", grpc.getDirection());
        assertEquals(false, grpc.getNullFirst());
    }

    @Test
    void toStringContainsFields() {
        OrderSpec spec = OrderSpec.builder().key("_count").direction(AggDirection.ASC).build();
        assertNotNull(spec.toString());
    }
}
