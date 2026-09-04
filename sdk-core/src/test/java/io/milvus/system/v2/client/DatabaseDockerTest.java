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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class DatabaseDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testDatabase() {
        // get current database
        ListDatabasesResp listDatabasesResp = client.listDatabases();
        List<String> dbNames = listDatabasesResp.getDatabaseNames();
        String currentDbName = "default";
        Assertions.assertTrue(dbNames.contains(currentDbName));

        // create a temp database
        String tempDatabaseName = "db_temp";
        // the shared standalone may still hold a db_temp left by another test class
        if (dbNames.contains(tempDatabaseName)) {
            client.dropDatabase(DropDatabaseReq.builder()
                    .databaseName(tempDatabaseName)
                    .build());
        }
        Map<String, String> properties = new HashMap<>();
        properties.put(Constant.DATABASE_REPLICA_NUMBER, "5");
        CreateDatabaseReq createDatabaseReq = CreateDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .properties(properties)
                .build();
        client.createDatabase(createDatabaseReq);

        listDatabasesResp = client.listDatabases();
        dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertTrue(dbNames.contains(tempDatabaseName));

        DescribeDatabaseResp descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        Map<String, String> propertiesResp = descDBResp.getProperties();
        Assertions.assertTrue(propertiesResp.containsKey(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertEquals("5", propertiesResp.get(Constant.DATABASE_REPLICA_NUMBER));

        // alter the database
        properties.put(Constant.DATABASE_REPLICA_NUMBER, "10");
        client.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName(tempDatabaseName)
                .properties(properties)
                .property("prop", "val")
                .build());
        descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        propertiesResp = descDBResp.getProperties();
        Assertions.assertTrue(propertiesResp.containsKey(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertEquals("10", propertiesResp.get(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertTrue(propertiesResp.containsKey("prop"));
        Assertions.assertEquals("val", propertiesResp.get("prop"));

        // drop property
        client.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                .databaseName(tempDatabaseName)
                .propertyKeys(Collections.singletonList("prop"))
                .build());
        descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        propertiesResp = descDBResp.getProperties();
        Assertions.assertFalse(propertiesResp.containsKey("prop"));

        // switch to the temp database
        Assertions.assertDoesNotThrow(() -> client.useDatabase(tempDatabaseName));

        // create a collection in the temp database
        String randomCollectionName = generator.generate(10);
        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createCollection(requestCreate);

        // switch to the default database
        Assertions.assertDoesNotThrow(() -> client.useDatabase(currentDbName));

        // list collections in the temp database
        ListCollectionsResp listCollectionsResp = client.listCollectionsV2(ListCollectionsReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        List<String> collectionNames = listCollectionsResp.getCollectionNames();
        Assertions.assertEquals(1, collectionNames.size());
        Assertions.assertTrue(collectionNames.contains(randomCollectionName));

        // drop the collection so that we can drop the temp database later
        client.dropCollection(DropCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build());

        // drop the temp database
        client.dropDatabase(DropDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());

        // check the temp database is deleted
        listDatabasesResp = client.listDatabases();
        dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertFalse(dbNames.contains(tempDatabaseName));
    }

}
