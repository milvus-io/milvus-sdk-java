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

package io.milvus.v2.service.collection;

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.Status;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.AlterCollectionFieldReq;
import io.milvus.v2.service.collection.request.AlterCollectionPropertiesReq;
import io.milvus.v2.service.collection.request.DropCollectionPropertiesReq;
import io.milvus.v2.service.collection.request.RenameCollectionReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.utils.VectorUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CollectionServiceSchemaCacheTest {
    private static final String ENDPOINT = "host:19530";
    private static final String DATABASE = "db";
    private static final String COLLECTION = "coll";

    @BeforeEach
    @AfterEach
    void clearCache() {
        SchemaCache.getInstance().clear();
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void collectionPropertiesPreserveCacheAndFieldPropertiesInvalidateIt() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        when(stub.alterCollection(any())).thenReturn(success);
        when(stub.alterCollectionField(any())).thenReturn(success);

        CollectionService service = new CollectionService();
        service.setEndpoint(ENDPOINT);
        service.setCurrentDbName("default");
        DescribeCollectionResponse cached = DescribeCollectionResponse.newBuilder().setCollectionID(1L).build();
        SchemaCache.getInstance().set(ENDPOINT, DATABASE, COLLECTION, cached);

        service.alterCollectionProperties(stub, AlterCollectionPropertiesReq.builder()
                .databaseName(DATABASE)
                .collectionName(COLLECTION)
                .property("key", "value")
                .build());
        assertSame(cached, SchemaCache.getInstance().get(ENDPOINT, DATABASE, COLLECTION));

        service.dropCollectionProperties(stub, DropCollectionPropertiesReq.builder()
                .databaseName(DATABASE)
                .collectionName(COLLECTION)
                .propertyKeys(Collections.singletonList("key"))
                .build());
        assertSame(cached, SchemaCache.getInstance().get(ENDPOINT, DATABASE, COLLECTION));

        service.alterCollectionField(stub, AlterCollectionFieldReq.builder()
                .databaseName(DATABASE)
                .collectionName(COLLECTION)
                .fieldName("field")
                .property("key", "value")
                .build());
        assertNull(SchemaCache.getInstance().get(ENDPOINT, DATABASE, COLLECTION));
    }

    @Test
    void crossDatabaseRenamePreservesTimestampForSessionRead() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.renameCollection(any())).thenReturn(Status.newBuilder().setCode(0).build());

        CollectionService service = new CollectionService();
        service.setEndpoint(ENDPOINT);
        service.setCurrentDbName("default");
        CollectionTsCache.getInstance().set(ENDPOINT, "source", "old", 100L);

        service.renameCollection(stub, RenameCollectionReq.builder()
                .databaseName("source")
                .collectionName("old")
                .targetDbName("target")
                .newCollectionName("new")
                .build());

        assertEquals(0L, CollectionTsCache.getInstance().get(ENDPOINT, "source", "old"));
        assertEquals(100L, CollectionTsCache.getInstance().get(ENDPOINT, "target", "new"));

        VectorUtils vectorUtils = new VectorUtils();
        vectorUtils.setEndpoint(ENDPOINT);
        SearchRequest request = vectorUtils.ConvertToGrpcSearchRequest(SearchReq.builder()
                .databaseName("target")
                .collectionName("new")
                .data(Collections.singletonList(new FloatVec(Collections.singletonList(1.0f))))
                .limit(1)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());
        assertEquals(100L, request.getGuaranteeTimestamp());
    }
}
