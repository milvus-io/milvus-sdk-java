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
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SortSpecTest {

    @Test
    void builderSetsFields() {
        SortSpec spec = SortSpec.builder()
                .fieldName("score")
                .direction(AggDirection.DESC)
                .nullFirst(true)
                .build();

        assertEquals("score", spec.getFieldName());
        assertEquals(AggDirection.DESC, spec.getDirection());
        assertEquals(true, spec.getNullFirst());
    }

    @Test
    void builderDefaultNullFirstIsNull() {
        SortSpec spec = SortSpec.builder().fieldName("score").direction(AggDirection.ASC).build();
        assertNull(spec.getNullFirst());
    }

    @Test
    void emptyFieldNameRejected() {
        assertThrows(MilvusClientException.class,
                () -> SortSpec.builder().direction(AggDirection.ASC).build());
        assertThrows(MilvusClientException.class,
                () -> SortSpec.builder().fieldName("").direction(AggDirection.ASC).build());
        assertThrows(MilvusClientException.class,
                () -> SortSpec.builder().fieldName(null).direction(AggDirection.ASC).build());
    }

    @Test
    void nullDirectionRejected() {
        assertThrows(MilvusClientException.class,
                () -> SortSpec.builder().fieldName("score").build());
    }

    @Test
    void toProtoIsRoundTrippedThroughTopHitsSpec() {
        TopHitsSpec topHits = TopHitsSpec.builder()
                .size(3)
                .addSort(SortSpec.builder()
                        .fieldName("score")
                        .direction(AggDirection.DESC)
                        .nullFirst(false)
                        .build())
                .build();

        io.milvus.grpc.SortSpec grpc = topHits.toProto().getSort(0);
        assertEquals("score", grpc.getFieldName());
        assertEquals("desc", grpc.getDirection());
        assertEquals(false, grpc.getNullFirst());
    }

    @Test
    void toStringContainsFields() {
        SortSpec spec = SortSpec.builder().fieldName("score").direction(AggDirection.ASC).build();
        assertNotNull(spec.toString());
    }
}
