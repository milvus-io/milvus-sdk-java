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
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.SearchRequest;
import io.milvus.param.dml.SearchParam;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class ParamUtilsTest {
    private final CollectionTsCache timestampCache = CollectionTsCache.getInstance();

    @BeforeEach
    @AfterEach
    void clearTimestampCache() {
        timestampCache.clear();
    }

    @Test
    void searchConversionUsesOnlyTheProvidedEndpoint() {
        timestampCache.set("host-a:19530", "db", "coll", 100L);
        timestampCache.set("host-b:19530", "db", "coll", 200L);
        timestampCache.set("host-c:19530", "other-db", "coll", 300L);

        SearchParam searchParam = SearchParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .withConsistencyLevel(ConsistencyLevelEnum.SESSION)
                .build();

        SearchRequest sessionRequest = ParamUtils.convertSearchParam(
                searchParam, "host-a:19530", "db");

        SearchParam defaultConsistencyParam = SearchParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();
        SearchRequest defaultConsistencyRequest = ParamUtils.convertSearchParam(
                defaultConsistencyParam, "host-a:19530", "db");

        assertEquals(100L, sessionRequest.getGuaranteeTimestamp());
        assertEquals(100L, defaultConsistencyRequest.getGuaranteeTimestamp());
    }
}
