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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TopHitsSpecTest {

    @Test
    void builderSetsSizeAndSort() {
        SortSpec sort = SortSpec.builder().fieldName("score").direction(AggDirection.DESC).build();
        TopHitsSpec spec = TopHitsSpec.builder()
                .size(3)
                .sort(Collections.singletonList(sort))
                .build();

        assertEquals(3, spec.getSize());
        assertEquals(Collections.singletonList(sort), spec.getSort());
    }

    @Test
    void builderAddSortAccumulates() {
        TopHitsSpec spec = TopHitsSpec.builder()
                .size(2)
                .addSort(SortSpec.builder().fieldName("a").direction(AggDirection.ASC).build())
                .addSort(SortSpec.builder().fieldName("b").direction(AggDirection.DESC).build())
                .build();

        assertEquals(Arrays.asList("a", "b"),
                Arrays.asList(spec.getSort().get(0).getFieldName(), spec.getSort().get(1).getFieldName()));
    }

    @Test
    void builderDefaultSortEmpty() {
        TopHitsSpec spec = TopHitsSpec.builder().size(1).build();
        assertTrue(spec.getSort().isEmpty());
    }

    @Test
    void nonPositiveSizeRejected() {
        assertThrows(MilvusClientException.class, () -> TopHitsSpec.builder().build());
        assertThrows(MilvusClientException.class, () -> TopHitsSpec.builder().size(0).build());
        assertThrows(MilvusClientException.class, () -> TopHitsSpec.builder().size(-1).build());
    }

    @Test
    void addSortRejectsNull() {
        assertThrows(MilvusClientException.class,
                () -> TopHitsSpec.builder().size(1).addSort(null));
    }

    @Test
    void sortListIsUnmodifiable() {
        TopHitsSpec spec = TopHitsSpec.builder().size(1).build();
        assertThrows(UnsupportedOperationException.class, () -> spec.getSort().add(null));
    }

    @Test
    void toProtoMapsSizeAndSort() {
        TopHitsSpec spec = TopHitsSpec.builder()
                .size(4)
                .addSort(SortSpec.builder().fieldName("pk").direction(AggDirection.ASC).build())
                .build();

        io.milvus.grpc.TopHitsSpec grpc = spec.toProto();
        assertEquals(4, grpc.getSize());
        assertEquals(1, grpc.getSortCount());
        assertEquals("pk", grpc.getSort(0).getFieldName());
    }

    @Test
    void toStringContainsFields() {
        TopHitsSpec spec = TopHitsSpec.builder().size(1).build();
        assertNotNull(spec.toString());
    }
}
