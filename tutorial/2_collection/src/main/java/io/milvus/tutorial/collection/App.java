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

package io.milvus.tutorial.collection;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.AlterCollectionPropertiesReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionPropertiesReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.collection.request.RenameCollectionReq;
import io.milvus.v2.service.collection.request.TruncateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;

import java.util.Arrays;
import java.util.List;

/**
 * Tutorial 2: Collection management.
 *
 * <p>Walks through the full collection lifecycle: create with a description and vector index,
 * inspect with {@code hasCollection} / {@code describeCollection}, change properties, load and
 * release, get load state and row count, rename, truncate, and finally drop.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final String TTL_SECONDS = "collection.ttl.seconds";

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String collectionName = "tutorial_collection";
            String renamedName = collectionName + "_renamed";

            // Pre-drop any collection left behind by an interrupted run; dropping a non-existent
            // collection succeeds, so this keeps the tutorial rerunnable.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(renamedName)
                    .build());

            printCollections(client, "Collections before the tutorial");

            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("title")
                    .dataType(DataType.VarChar)
                    .maxLength(256)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(4)
                    .build());

            // createCollection creates the collection with its schema, a vector index, and a
            // default consistency level.
            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .description("Collection created by tutorial/2_collection")
                    .collectionSchema(schema)
                    .indexParams(Arrays.asList(IndexParam.builder()
                            .fieldName("embedding")
                            .indexType(IndexParam.IndexType.AUTOINDEX)
                            .build()))
                    .consistencyLevel(ConsistencyLevel.BOUNDED)
                    .build());
            System.out.printf("Collection '%s' created%n", collectionName);

            boolean exists = client.hasCollection(HasCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.printf("hasCollection('%s') = %s%n", collectionName, exists);

            describeCollection(client, collectionName);

            // alterCollectionProperties adds or replaces key/value settings. This property asks
            // Milvus to expire entities after 3600 seconds.
            client.alterCollectionProperties(AlterCollectionPropertiesReq.builder()
                    .collectionName(collectionName)
                    .property(TTL_SECONDS, "3600")
                    .build());
            System.out.println("Set " + TTL_SECONDS + " = 3600");
            describeProperty(client, collectionName);

            client.dropCollectionProperties(DropCollectionPropertiesReq.builder()
                    .collectionName(collectionName)
                    .propertyKeys(Arrays.asList(TTL_SECONDS))
                    .build());
            System.out.println("Removed " + TTL_SECONDS);
            describeProperty(client, collectionName);

            // loadCollection makes the data and index searchable in memory.
            client.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.println("Collection loaded");

            Boolean loadState = client.getLoadState(GetLoadStateReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.println("Load state: " + loadState);

            GetCollectionStatsResp stats = client.getCollectionStats(GetCollectionStatsReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.println("Row count: " + stats.getNumOfEntities());

            client.releaseCollection(ReleaseCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.println("Collection released");

            // renameCollection moves the collection to a new name.
            client.renameCollection(RenameCollectionReq.builder()
                    .collectionName(collectionName)
                    .newCollectionName(renamedName)
                    .build());
            boolean oldExists = client.hasCollection(HasCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            boolean newExists = client.hasCollection(HasCollectionReq.builder()
                    .collectionName(renamedName)
                    .build());
            System.out.printf("After rename: old name exists=%s, new name exists=%s%n",
                    oldExists, newExists);

            // truncateCollection removes all entities but keeps the schema and indexes.
            client.truncateCollection(TruncateCollectionReq.builder()
                    .collectionName(renamedName)
                    .build());
            System.out.println("Truncate removes all entities but preserves the collection schema and indexes.");

            // Clean up: drop the renamed collection.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(renamedName)
                    .build());
            System.out.printf("Collection '%s' dropped%n", renamedName);

            printCollections(client, "Collections after cleanup");
        } finally {
            client.close();
        }
    }

    private static void describeCollection(MilvusClientV2 client, String collectionName) {
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.printf("describeCollection: name=%s id=%d primaryField=%s vectorFields=%s%n",
                resp.getCollectionName(), resp.getCollectionID(), resp.getPrimaryFieldName(),
                resp.getVectorFieldNames());
    }

    private static void describeProperty(MilvusClientV2 client, String collectionName) {
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println("Property " + TTL_SECONDS + " = "
                + resp.getProperties().getOrDefault(TTL_SECONDS, "<not set>"));
    }

    private static void printCollections(MilvusClientV2 client, String heading) {
        ListCollectionsResp resp = client.listCollections();
        List<String> names = resp.getCollectionNames();
        System.out.printf("%s: %s%n", heading, names.isEmpty() ? "<none>" : String.join(", ", names));
    }
}
