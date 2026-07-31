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

package io.milvus.v2.client.globalcluster;

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.QueryRequest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.QueryReq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobalClusterCacheTest {
    @AfterEach
    void clearCache() {
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void primaryChangePreservesLogicalGlobalEndpointForSessionTimestamp() throws Exception {
        String logicalEndpoint = "global.example.com:443";
        MilvusClientV2 globalClient = new MilvusClientV2(null);
        setField(globalClient, "connectConfig", ConnectConfig.builder()
                .uri("https://global.example.com:443")
                .dbName("db")
                .build());
        setField(globalClient, "cacheEndpoint", logicalEndpoint);

        MilvusClientV2 newPrimary = new MilvusClientV2(null);
        setField(newPrimary, "cacheEndpoint", "primary-b.example.com:443");
        CollectionTsCache.getInstance().set(logicalEndpoint, "db", "coll", 100L);

        Method updatePrimary = MilvusClientV2.class.getDeclaredMethod("updatePrimaryConnection", MilvusClientV2.class);
        updatePrimary.setAccessible(true);
        updatePrimary.invoke(globalClient, newPrimary);

        assertEquals(logicalEndpoint, getField(globalClient, "cacheEndpoint"));
        VectorService vectorService = (VectorService) getField(globalClient, "vectorService");
        QueryRequest query = vectorService.vectorUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .collectionName("coll")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());
        assertEquals(100L, query.getGuaranteeTimestamp());
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
