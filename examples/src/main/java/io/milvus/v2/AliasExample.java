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
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.AlterAliasReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;

import java.util.List;

/**
 * Demonstrates the alias management APIs on {@link MilvusClientV2}: createAlias, listAliases,
 * describeAlias, alterAlias and dropAlias.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below).</p>
 */
public class AliasExample {
    private static final String COLLECTION_NAME = "java_sdk_example_alias_v2";
    private static final String ALIAS_NAME = "java_sdk_example_alias";

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            createCollection(client);
            createAlias(client);
            listAliases(client);
            describeAlias(client);
            alterAlias(client);
            describeAlias(client);
            dropAlias(client);
        } finally {
            try {
                client.dropAlias(DropAliasReq.builder()
                        .alias(ALIAS_NAME)
                        .build());
            } catch (Exception e) {
                // alias may already be gone; ignore
            }
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            try {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(COLLECTION_NAME + "_2")
                        .build());
            } catch (Exception e) {
                // collection may not exist; ignore
            }
            client.close(5);
        }
    }

    private static void createCollection(MilvusClientV2 client) {
        System.out.println("========== Create collection ==========");
        // Clean up leftovers from a previous run: the alias (if any) must be dropped before
        // either collection can be dropped, since an alias blocks dropping its target.
        try {
            client.dropAlias(DropAliasReq.builder()
                    .alias(ALIAS_NAME)
                    .build());
        } catch (Exception e) {
            // alias does not exist; ignore
        }
        try {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME + "_2")
                    .build());
        } catch (Exception e) {
            // collection does not exist; ignore
        }
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(4)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .build());
        System.out.println("Collection '" + COLLECTION_NAME + "' created");
    }

    private static void createAlias(MilvusClientV2 client) {
        System.out.println("========== createAlias() ==========");
        client.createAlias(CreateAliasReq.builder()
                .collectionName(COLLECTION_NAME)
                .alias(ALIAS_NAME)
                .build());
        System.out.println("Alias '" + ALIAS_NAME + "' created for collection '" + COLLECTION_NAME + "'");
    }

    private static void listAliases(MilvusClientV2 client) {
        System.out.println("========== listAliases() ==========");
        ListAliasResp resp = client.listAliases(ListAliasesReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        List<String> aliases = resp.getAlias();
        System.out.println("Aliases of '" + COLLECTION_NAME + "': " + aliases);
    }

    private static void describeAlias(MilvusClientV2 client) {
        System.out.println("========== describeAlias() ==========");
        DescribeAliasResp resp = client.describeAlias(DescribeAliasReq.builder()
                .alias(ALIAS_NAME)
                .build());
        System.out.println("Alias '" + resp.getAlias() + "' -> collection '" + resp.getCollectionName()
                + "' in database '" + resp.getDatabaseName() + "'");
    }

    private static void alterAlias(MilvusClientV2 client) {
        System.out.println("========== alterAlias() ==========");
        // Create a second collection and repoint the alias to it.
        String secondCollection = COLLECTION_NAME + "_2";
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(4)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(secondCollection)
                .collectionSchema(schema)
                .build());

        client.alterAlias(AlterAliasReq.builder()
                .collectionName(secondCollection)
                .alias(ALIAS_NAME)
                .build());
        System.out.println("Alias '" + ALIAS_NAME + "' repointed to collection '" + secondCollection + "'");
    }

    private static void dropAlias(MilvusClientV2 client) {
        System.out.println("========== dropAlias() ==========");
        client.dropAlias(DropAliasReq.builder()
                .alias(ALIAS_NAME)
                .build());
        System.out.println("Alias '" + ALIAS_NAME + "' dropped");
    }
}
