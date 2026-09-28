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

package io.milvus.v2;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.AlterDatabasePropertiesReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabasePropertiesReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;

import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates the database management APIs on {@link MilvusClientV2}: listDatabases,
 * createDatabase, describeDatabase, useDatabase, currentUsedDatabase and dropDatabase.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below).</p>
 */
public class DatabaseExample {
    private static final String DB_NAME = "java_sdk_example_db";
    private static final String COLLECTION_NAME = "java_sdk_example_db_collection";

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            listDatabases(client);
            createDatabase(client);
            describeDatabase(client);
            alterAndDropProperties(client);
            useDatabase(client);
            dropDatabase(client);
        } finally {
            client.close(5);
        }
    }

    private static void listDatabases(MilvusClientV2 client) {
        System.out.println("========== listDatabases() ==========");
        ListDatabasesResp resp = client.listDatabases();
        System.out.println("Databases: " + resp.getDatabaseNames());
    }

    private static void createDatabase(MilvusClientV2 client) {
        System.out.println("========== createDatabase() ==========");
        // Clean up a leftover database from a previous run, then recreate it.
        try {
            client.dropDatabase(DropDatabaseReq.builder()
                    .databaseName(DB_NAME)
                    .build());
        } catch (Exception e) {
            // database does not exist; ignore
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("database.replica.number", "1");
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(DB_NAME)
                .properties(properties)
                .build());
        System.out.println("Database '" + DB_NAME + "' created with properties: " + properties);
    }

    private static void describeDatabase(MilvusClientV2 client) {
        System.out.println("========== describeDatabase() ==========");
        DescribeDatabaseResp resp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(DB_NAME)
                .build());
        System.out.println(resp);
    }

    private static void alterAndDropProperties(MilvusClientV2 client) {
        System.out.println("========== alterDatabaseProperties() ==========");
        Map<String, String> properties = new HashMap<>();
        properties.put("database.max.collections", "20");
        client.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName(DB_NAME)
                .properties(properties)
                .build());
        System.out.println("Altered database properties: " + properties);

        System.out.println("========== dropDatabaseProperties() ==========");
        client.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                .databaseName(DB_NAME)
                .propertyKeys(java.util.Collections.singletonList("database.max.collections"))
                .build());
        System.out.println("Dropped database property 'database.max.collections'");
    }

    private static void useDatabase(MilvusClientV2 client) throws InterruptedException {
        System.out.println("========== useDatabase() ==========");
        client.useDatabase(DB_NAME);
        System.out.println("Current database: " + client.currentUsedDatabase());

        // The collection created below lives in DB_NAME instead of the default database.
        System.out.println("========== Create collection in current database ==========");
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(io.milvus.v2.service.collection.request.AddFieldReq.builder()
                .fieldName("id")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .build());
        schema.addField(io.milvus.v2.service.collection.request.AddFieldReq.builder()
                .fieldName("vector")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(4)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .databaseName(DB_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .build());
        System.out.println("Collection '" + COLLECTION_NAME + "' created in database '" + DB_NAME + "'");

        client.dropCollection(DropCollectionReq.builder()
                .databaseName(DB_NAME)
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");

        // Switch back to the default database before dropping DB_NAME.
        client.useDatabase("default");
        System.out.println("Current database: " + client.currentUsedDatabase());
    }

    private static void dropDatabase(MilvusClientV2 client) {
        System.out.println("========== dropDatabase() ==========");
        client.dropDatabase(DropDatabaseReq.builder()
                .databaseName(DB_NAME)
                .build());
        System.out.println("Database '" + DB_NAME + "' dropped");
    }
}
