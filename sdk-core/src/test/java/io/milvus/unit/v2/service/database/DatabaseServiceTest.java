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

package io.milvus.unit.v2.service.database;

import io.milvus.grpc.AlterDatabaseRequest;
import io.milvus.grpc.CreateDatabaseRequest;
import io.milvus.grpc.DescribeDatabaseRequest;
import io.milvus.grpc.DescribeDatabaseResponse;
import io.milvus.grpc.DropDatabaseRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.v2.service.database.DatabaseService;
import io.milvus.v2.service.database.request.AlterDatabasePropertiesReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabasePropertiesReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class DatabaseServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private DatabaseService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new DatabaseService();
        service.setEndpoint("host:19530");
        service.setCurrentDbName("db");
    }

    @AfterEach
    void tearDown() {
        io.milvus.common.utils.cache.SchemaCache.getInstance().clear();
        io.milvus.common.utils.cache.CollectionTsCache.getInstance().clear();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void createDatabaseReturnsNullAndBuildsProperties() {
        when(stub.createDatabase(any())).thenReturn(success());
        Map<String, String> properties = new HashMap<>();
        properties.put("key", "value");
        CreateDatabaseReq request = CreateDatabaseReq.builder()
                .databaseName("new_db")
                .properties(properties)
                .build();

        assertNull(service.createDatabase(stub, request));

        ArgumentCaptor<CreateDatabaseRequest> captor = ArgumentCaptor.forClass(CreateDatabaseRequest.class);
        verify(stub).createDatabase(captor.capture());
        assertEquals("new_db", captor.getValue().getDbName());
        assertEquals("value", captor.getValue().getProperties(0).getValue());
    }

    @Test
    void createDatabaseWithoutPropertiesStillSucceeds() {
        when(stub.createDatabase(any())).thenReturn(success());
        CreateDatabaseReq request = CreateDatabaseReq.builder().databaseName("new_db").build();

        assertNull(service.createDatabase(stub, request));
        verify(stub).createDatabase(any(CreateDatabaseRequest.class));
    }

    @Test
    void dropDatabaseReturnsNull() {
        when(stub.dropDatabase(any())).thenReturn(success());
        DropDatabaseReq request = DropDatabaseReq.builder().databaseName("db_to_drop").build();

        assertNull(service.dropDatabase(stub, request));

        ArgumentCaptor<DropDatabaseRequest> captor = ArgumentCaptor.forClass(DropDatabaseRequest.class);
        verify(stub).dropDatabase(captor.capture());
        assertEquals("db_to_drop", captor.getValue().getDbName());
    }

    @Test
    void listDatabasesReturnsNames() {
        ListDatabasesResponse response = ListDatabasesResponse.newBuilder()
                .setStatus(success())
                .addAllDbNames(Arrays.asList("default", "db1"))
                .build();
        when(stub.listDatabases(any())).thenReturn(response);

        ListDatabasesResp result = service.listDatabases(stub);

        assertEquals(Arrays.asList("default", "db1"), result.getDatabaseNames());
        verify(stub).listDatabases(any(ListDatabasesRequest.class));
    }

    @Test
    void alterDatabasePropertiesReturnsNull() {
        when(stub.alterDatabase(any())).thenReturn(success());
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder()
                .databaseName("db")
                .property("quota", "100")
                .build();

        assertNull(service.alterDatabaseProperties(stub, request));

        ArgumentCaptor<AlterDatabaseRequest> captor = ArgumentCaptor.forClass(AlterDatabaseRequest.class);
        verify(stub).alterDatabase(captor.capture());
        assertEquals("db", captor.getValue().getDbName());
        assertEquals("quota", captor.getValue().getProperties(0).getKey());
    }

    @Test
    void dropDatabasePropertiesReturnsNull() {
        when(stub.alterDatabase(any())).thenReturn(success());
        DropDatabasePropertiesReq request = DropDatabasePropertiesReq.builder()
                .databaseName("db")
                .propertyKeys(Collections.singletonList("quota"))
                .build();

        assertNull(service.dropDatabaseProperties(stub, request));

        ArgumentCaptor<AlterDatabaseRequest> captor = ArgumentCaptor.forClass(AlterDatabaseRequest.class);
        verify(stub).alterDatabase(captor.capture());
        assertEquals("db", captor.getValue().getDbName());
        assertFalse(captor.getValue().getDeleteKeysList().isEmpty());
    }

    @Test
    void describeDatabaseReturnsNameAndProperties() {
        DescribeDatabaseResponse response = DescribeDatabaseResponse.newBuilder()
                .setStatus(success())
                .setDbName("db")
                .addProperties(KeyValuePair.newBuilder().setKey("k1").setValue("v1").build())
                .build();
        when(stub.describeDatabase(any())).thenReturn(response);

        DescribeDatabaseResp result = service.describeDatabase(stub,
                DescribeDatabaseReq.builder().databaseName("db").build());

        assertEquals("db", result.getDatabaseName());
        assertEquals("v1", result.getProperties().get("k1"));
        verify(stub).describeDatabase(any(DescribeDatabaseRequest.class));
    }

    @Test
    void describeDatabaseWithoutPropertiesReturnsEmptyMap() {
        DescribeDatabaseResponse response = DescribeDatabaseResponse.newBuilder()
                .setStatus(success())
                .setDbName("db")
                .build();
        when(stub.describeDatabase(any())).thenReturn(response);

        DescribeDatabaseResp result = service.describeDatabase(stub,
                DescribeDatabaseReq.builder().databaseName("db").build());

        assertEquals("db", result.getDatabaseName());
        assertTrue(result.getProperties().isEmpty());
    }
}
