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

package io.milvus.unit.v1.param;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.MetricType;
import io.milvus.param.dml.AnnSearchParam;
import io.milvus.param.dml.HybridSearchParam;
import io.milvus.param.dml.ranker.RRFRanker;
import io.milvus.param.dml.ranker.WeightedRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class HybridSearchParamTest {

    private AnnSearchParam vectorRequest(int count) {
        return AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.nCopies(count, Arrays.asList(0.1F, 0.2F)))
                .build();
    }

    @Test
    void builderSetsAllFields() {
        AnnSearchParam req1 = vectorRequest(1);
        AnnSearchParam req2 = vectorRequest(1);
        RRFRanker ranker = RRFRanker.newBuilder().withK(60).build();

        HybridSearchParam param = HybridSearchParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .addSearchRequest(req1)
                .addSearchRequest(req2)
                .withRanker(ranker)
                .withLimit(10L)
                .withOutFields(Arrays.asList("a", "b"))
                .withOffset(3L)
                .withRoundDecimal(2)
                .withGroupByFieldName("type")
                .withGroupSize(2)
                .withStrictGroupSize(true)
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals(Arrays.asList(req1, req2), param.getSearchRequests());
        assertEquals(ranker, param.getRanker());
        assertEquals(10L, param.getTopK());
        assertEquals(Arrays.asList("a", "b"), param.getOutFields());
        assertEquals(3L, param.getOffset());
        assertEquals(2, param.getRoundDecimal());
        assertEquals("type", param.getGroupByFieldName());
        assertEquals(2, param.getGroupSize());
        assertEquals(Boolean.TRUE, param.getStrictGroupSize());
        assertEquals(ConsistencyLevelEnum.BOUNDED, param.getConsistencyLevel());
    }

    @Test
    void builderDefaults() {
        HybridSearchParam param = HybridSearchParam.newBuilder()
                .withCollectionName("coll")
                .addSearchRequest(vectorRequest(1))
                .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                .withLimit(10L)
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getPartitionNames().isEmpty());
        assertTrue(param.getOutFields().isEmpty());
        assertEquals(0L, param.getOffset());
        assertEquals(-1, param.getRoundDecimal());
        assertNull(param.getConsistencyLevel());
        assertNull(param.getGroupByFieldName());
        assertNull(param.getGroupSize());
        assertNull(param.getStrictGroupSize());
    }

    @Test
    void withTopKIsDeprecatedButWorks() {
        HybridSearchParam param = HybridSearchParam.newBuilder()
                .withCollectionName("coll")
                .addSearchRequest(vectorRequest(1))
                .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                .withTopK(5)
                .build();

        assertEquals(5L, param.getTopK());
    }

    @Test
    void setDatabaseNameUpdatesDatabaseName() {
        HybridSearchParam param = HybridSearchParam.newBuilder()
                .withCollectionName("coll")
                .addSearchRequest(vectorRequest(1))
                .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                .withLimit(10L)
                .build();
        param.setDatabaseName("updated");

        assertEquals("updated", param.getDatabaseName());
    }

    @Test
    void missingRankerIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("coll")
                        .addSearchRequest(vectorRequest(1))
                        .withLimit(10L)
                        .build());
    }

    @Test
    void missingSearchRequestIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                        .withLimit(10L)
                        .build());
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("")
                        .addSearchRequest(vectorRequest(1))
                        .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                        .withLimit(10L)
                        .build());
    }

    @Test
    void zeroTopKIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("coll")
                        .addSearchRequest(vectorRequest(1))
                        .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                        .withLimit(0L)
                        .build());
    }

    @Test
    void mismatchedVectorCountsAcrossRequestsAreRejected() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("coll")
                        .addSearchRequest(vectorRequest(1))
                        .addSearchRequest(vectorRequest(2))
                        .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                        .withLimit(10L)
                        .build());
    }

    @Test
    void negativeGroupSizeIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> HybridSearchParam.newBuilder()
                        .withCollectionName("coll")
                        .addSearchRequest(vectorRequest(1))
                        .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                        .withLimit(10L)
                        .withGroupByFieldName("type")
                        .withGroupSize(0)
                        .build());
    }

    @Test
    void nullParamValuesAreRejected() {
        HybridSearchParam.Builder builder = HybridSearchParam.newBuilder()
                .withCollectionName("coll")
                .addSearchRequest(vectorRequest(1))
                .withRanker(new WeightedRankerBuilderHelper().buildWeighted())
                .withLimit(10L);

        assertThrows(IllegalArgumentException.class, () -> builder.withPartitionNames(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addPartitionName(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addSearchRequest(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withRanker(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withLimit(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withTopK(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withOutFields(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addOutField(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withOffset(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withRoundDecimal(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withGroupByFieldName(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withGroupSize(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withStrictGroupSize(null));
    }

    private static class WeightedRankerBuilderHelper {
        WeightedRanker buildWeighted() {
            return WeightedRanker.newBuilder()
                    .withWeights(Arrays.asList(0.5F, 0.5F))
                    .build();
        }
    }
}
