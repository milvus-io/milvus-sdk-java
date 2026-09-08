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

package io.milvus.tutorial.database;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.database.request.AlterDatabasePropertiesReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabasePropertiesReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;

import java.util.Arrays;
import java.util.List;

/**
 * Tutorial 7: Database management.
 *
 * <p>Walks through logical database administration with {@code MilvusClientV2}: list existing
 * databases, create a database, inspect it, set and remove a property, switch the client's
 * selected database with {@code useDatabase}, and finally drop the tutorial database.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final String REPLICA_NUMBER = "database.replica.number";

    public static void main(String[] args) throws InterruptedException {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String databaseName = "tutorial_database";

            // Pre-drop any database left behind by an interrupted run so the tutorial stays
            // rerunnable. Drop only when it exists: dropDatabase is not idempotent on Milvus
            // 2.6.0-2.6.5 (missing-database handling landed in 2.6.6), so an unconditional drop
            // would abort the first run on those versions.
            if (client.listDatabases().getDatabaseNames().contains(databaseName)) {
                client.dropDatabase(DropDatabaseReq.builder()
                        .databaseName(databaseName)
                        .build());
            }

            printDatabases(client, "Databases before the tutorial");

            // createDatabase creates a logical database. The name is used by later requests and by
            // useDatabase.
            client.createDatabase(CreateDatabaseReq.builder()
                    .databaseName(databaseName)
                    .build());
            System.out.printf("Database '%s' created%n", databaseName);

            // describeDatabase returns metadata for the named database.
            DescribeDatabaseResp descResp = client.describeDatabase(DescribeDatabaseReq.builder()
                    .databaseName(databaseName)
                    .build());
            System.out.printf("describeDatabase: name=%s properties=%s%n",
                    descResp.getDatabaseName(), descResp.getProperties());

            // alterDatabaseProperties adds or replaces property key/value pairs. This setting
            // configures the default replica count for collections in the database.
            client.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                    .databaseName(databaseName)
                    .property(REPLICA_NUMBER, "1")
                    .build());
            System.out.println("Set " + REPLICA_NUMBER + " = 1");
            printProperty(client, databaseName);

            // dropDatabaseProperties removes the specified key while preserving other properties.
            client.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                    .databaseName(databaseName)
                    .propertyKeys(Arrays.asList(REPLICA_NUMBER))
                    .build());
            System.out.println("Removed " + REPLICA_NUMBER);
            printProperty(client, databaseName);

            // useDatabase selects this database for requests that omit an explicit database name.
            client.useDatabase(databaseName);
            System.out.println("Selected database for subsequent requests: " + databaseName);

            // Clean up: switch back to the default database, then drop the tutorial database.
            client.useDatabase("default");
            client.dropDatabase(DropDatabaseReq.builder()
                    .databaseName(databaseName)
                    .build());
            System.out.printf("Database '%s' dropped%n", databaseName);

            printDatabases(client, "Databases after cleanup");
        } finally {
            client.close();
        }
    }

    private static void printProperty(MilvusClientV2 client, String databaseName) {
        DescribeDatabaseResp resp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(databaseName)
                .build());
        System.out.println("Property " + REPLICA_NUMBER + " = "
                + resp.getProperties().getOrDefault(REPLICA_NUMBER, "<not set>"));
    }

    private static void printDatabases(MilvusClientV2 client, String heading) {
        ListDatabasesResp resp = client.listDatabases();
        List<String> names = resp.getDatabaseNames();
        System.out.printf("%s: %s%n", heading, names.isEmpty() ? "<none>" : String.join(", ", names));
    }
}
