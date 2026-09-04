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

package io.milvus.system.v1.client;

import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.dml.DeleteIdsParam;
import io.milvus.param.highlevel.dml.response.DeleteResponse;
import io.milvus.param.index.*;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Tag("system")
class DatabaseDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testDatabase() {
        String dbName = "test_database";
        CreateDatabaseParam createDatabaseParam = CreateDatabaseParam.newBuilder()
                .withDatabaseName(dbName)
                .withReplicaNumber(5)
                .build();
        R<RpcStatus> createResponse = client.createDatabase(createDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createResponse.getStatus().intValue());

        // check database props
        DescribeDatabaseParam describeDBParam = DescribeDatabaseParam.newBuilder().withDatabaseName(dbName).build();
        R<DescribeDatabaseResponse> describeResponse = client.describeDatabase(describeDBParam);
        Assertions.assertEquals(R.Status.Success.getCode(), describeResponse.getStatus().intValue());
        DescDBResponseWrapper describeDBWrapper = new DescDBResponseWrapper(describeResponse.getData());
        Assertions.assertEquals(dbName, describeDBWrapper.getDatabaseName());
        Assertions.assertEquals(5, describeDBWrapper.getReplicaNumber());

        // alter database props
        AlterDatabaseParam alterDatabaseParam = AlterDatabaseParam.newBuilder()
                .withDatabaseName(dbName)
                .withReplicaNumber(3)
                .build();
        R<RpcStatus> alterDatabaseResponse = client.alterDatabase(alterDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), alterDatabaseResponse.getStatus().intValue());

        // check database props
        describeResponse = client.describeDatabase(describeDBParam);
        Assertions.assertEquals(R.Status.Success.getCode(), describeResponse.getStatus().intValue());
        describeDBWrapper = new DescDBResponseWrapper(describeResponse.getData());
        Assertions.assertEquals(dbName, describeDBWrapper.getDatabaseName());
        Assertions.assertEquals(3, describeDBWrapper.getReplicaNumber());


        DropDatabaseParam dropDatabaseParam = DropDatabaseParam.newBuilder().withDatabaseName(dbName).build();
        R<RpcStatus> dropResponse = client.dropDatabase(dropDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropResponse.getStatus().intValue());
    }

    @Test
    void testCacheCollectionSchema() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // create a new db
        String testDbName = "test_database";
        CreateDatabaseParam createDatabaseParam = CreateDatabaseParam.newBuilder()
                .withDatabaseName(testDbName)
                .withReplicaNumber(1)
                .build();
        R<RpcStatus> dbResponse = client.createDatabase(createDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dbResponse.getStatus().intValue());

        // create a collection in the default db
        createSimpleCollection(client, "", randomCollectionName, "pk", false, DIMENSION, ConsistencyLevelEnum.BOUNDED);

        // a temp client connect to the new db
        ConnectParam connectParam = connectParamBuilder()
                .withAuthorization("root", "Milvus")
                .withDatabaseName(testDbName)
                .build();
        MilvusClientForTest tempClient = new MilvusClientForTest(connectParam);

        // use the temp client to insert correct data into the default collection
        // there will be an entry for this collection in the process-global schema cache
        // there will be a timestamp for this collection in the global timestamp cache
        JsonObject row = new JsonObject();
        row.addProperty("pk", 8);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
        R<MutationResult> insertR = tempClient.insert(InsertParam.newBuilder()
                .withDatabaseName("default")
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
        Assertions.assertEquals(1, insertR.getData().getInsertCnt());

        // check the schema cache of this collection, must be not null
        DescribeCollectionResponse descResp = tempClient.getCollectionInfo("default", randomCollectionName);
        Assertions.assertNotNull(descResp);

        // check the timestamp of this collection, must be positive
        long ts11 = CollectionTsCache.getInstance().get("localhost:29530", "default", randomCollectionName);
        Assertions.assertTrue(ts11 > 0L);

        // insert wrong data; the refreshed, valid collection schema remains cached
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
        insertR = tempClient.insert(InsertParam.newBuilder()
                .withDatabaseName("default")
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertNotEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
        descResp = tempClient.getCollectionInfo("default", randomCollectionName);
        Assertions.assertNotNull(descResp);

        // use the default client to do upsert correct data
        TimeUnit.MILLISECONDS.sleep(100);
        row.addProperty("pk", 999);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
        insertR = client.upsert(UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
        Assertions.assertEquals(1, insertR.getData().getUpsertCnt());

        // check the timestamp of this collection, must be a new positive
        long ts12 = CollectionTsCache.getInstance().get("localhost:29530", "default", randomCollectionName);
        Assertions.assertTrue(ts12 > ts11);

        // create a new collection with the same name, different schema, in the test db
        createSimpleCollection(tempClient, "", randomCollectionName, "aaa", false, 4, ConsistencyLevelEnum.BOUNDED);

        // use the temp client to insert wrong data, wrong dimension
        row.addProperty("aaa", 22);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
        insertR = tempClient.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertNotEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // check the timestamp of this collection, must be null
        long ts21 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
        Assertions.assertEquals(0L, ts21);

        // use the temp client to do upsert correct data
        TimeUnit.MILLISECONDS.sleep(100);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(4)));
        insertR = tempClient.upsert(UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
        Assertions.assertEquals(1, insertR.getData().getUpsertCnt());

        // check the schema cache of this collection, must be not null
        descResp = tempClient.getCollectionInfo(testDbName, randomCollectionName);
        Assertions.assertNotNull(descResp);

        // check the timestamp of this collection, must be positive
        long ts22 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
        Assertions.assertTrue(ts22 > 0L);

        // tempClient upsert wrong data
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
        insertR = tempClient.upsert(UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertNotEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // the refreshed, valid collection schema remains cached
        descResp = tempClient.getCollectionInfo(testDbName, randomCollectionName);
        Assertions.assertNotNull(descResp);

        // tempClient delete data
        R<DeleteResponse> delResp = tempClient.delete(DeleteIdsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addPrimaryId(22L)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), delResp.getStatus().intValue());

        // check the schema cache of this collection, must be not null
        descResp = tempClient.getCollectionInfo(testDbName, randomCollectionName);
        Assertions.assertNotNull(descResp);

        // check the timestamp of this collection, must be greater than previous
        long ts23 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
        Assertions.assertTrue(ts23 > ts22);

        // use the default client to drop the collection in the new db
        R<RpcStatus> dropResp = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDatabaseName(testDbName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropResp.getStatus().intValue());

        // check the timestamp of this collection, must be deleted
        long ts31 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
        Assertions.assertEquals(0L, ts31);

        // use the temp client to insert correct data into the collection
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(4)));
        insertR = tempClient.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertNotEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // check the timestamp of this collection, must be null
        long ts32 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
        Assertions.assertEquals(0L, ts32);

        // check the schema cache of this collection, must be null
        descResp = tempClient.getCollectionInfo(testDbName, randomCollectionName);
        Assertions.assertNull(descResp);
    }
}
