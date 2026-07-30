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

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.QueryRequest;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.QueryReq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VectorUtilsTest {
    private final CollectionTsCache timestampCache = CollectionTsCache.getInstance();

    @BeforeEach
    @AfterEach
    void clearTimestampCache() {
        timestampCache.clear();
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
}
