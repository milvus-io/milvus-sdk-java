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

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.common.RowPolicyType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.rls.request.CreateRowPolicyReq;
import io.milvus.v2.service.rls.request.DeleteRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.DropRowPolicyReq;
import io.milvus.v2.service.rls.request.GetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.ListRLSPrincipalsReq;
import io.milvus.v2.service.rls.request.ListRowPoliciesReq;
import io.milvus.v2.service.rls.request.SetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.UpdateRowPolicyReq;
import io.milvus.v2.service.rls.response.GetRLSPrincipalTagsResp;
import io.milvus.v2.service.rls.response.ListRLSPrincipalsResp;
import io.milvus.v2.service.rls.response.ListRowPoliciesResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

// Row-level security (RLS) lets you control which rows a principal can read or write.
// This example manages policies and principal tags, then shows how a request that
// carries an rlsPrincipal is enforced by Milvus (row filtering and write checks).
// Enable RBAC in Milvus and have the root user credentials ready.
// Read this doc to know more about RLS in milvus: https://milvus.io/docs/row-level-security.md
public class RLSExample {
    private static final String URI = "http://localhost:19530";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "Milvus";
    private static final String DATABASE_NAME = "default";
    private static final String COLLECTION_NAME = "java_sdk_example_rls_collection";
    private static final String DEMO_POLICY = "java_sdk_example_policy";
    private static final String ALICE = "alice";
    private static final String BOB = "bob";

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri(URI)
                .username(ROOT_USER)
                .password(ROOT_PASSWORD)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        try {
            cleanup(client);
            createCollection(client);
            runExample(client);
        } finally {
            cleanup(client);
            client.close();
        }
    }

    private static void createCollection(MilvusClientV2 client) {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("team")
                .dataType(DataType.VarChar)
                .maxLength(64)
                .build());
        CreateCollectionReq request = CreateCollectionReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .properties(Collections.singletonMap("rls.enabled", "true"))
                .build();
        client.createCollection(request);
        print("Collection created", COLLECTION_NAME);
    }

    private static void runExample(MilvusClientV2 client) {
        // A policy expression may reference the current principal and its tags:
        //   $current_principal_tags['team']  ->  the 'team' tag of the request principal
        client.createRowPolicy(CreateRowPolicyReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .policyName(DEMO_POLICY)
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.SEARCH, RowPolicyAction.QUERY, RowPolicyAction.INSERT))
                .usingExpr("team == $current_principal_tags['team']")
                .checkExpr("team == $current_principal_tags['team']")
                .description("A principal only sees or writes rows of its own team")
                .build());
        print("Row policy created", DEMO_POLICY);

        ListRowPoliciesResp policies = client.listRowPolicies(ListRowPoliciesReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .build());
        print("After create, row policies", policies.getPolicies());

        client.updateRowPolicy(UpdateRowPolicyReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .policyName(DEMO_POLICY)
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.SEARCH, RowPolicyAction.QUERY, RowPolicyAction.INSERT, RowPolicyAction.DELETE))
                .usingExpr("team == $current_principal_tags['team']")
                .checkExpr("team == $current_principal_tags['team']")
                .description("Updated: a principal sees or writes only rows of its own team")
                .build());
        print("Row policy updated", DEMO_POLICY);

        Map<String, Object> aliceTags = new LinkedHashMap<>();
        aliceTags.put("team", "engineering");
        aliceTags.put("level", 3);
        client.setRLSPrincipalTags(SetRLSPrincipalTagsReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .principalName(ALICE)
                .tags(aliceTags)
                .build());
        Map<String, Object> bobTags = new LinkedHashMap<>();
        bobTags.put("team", "marketing");
        client.setRLSPrincipalTags(SetRLSPrincipalTagsReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .principalName(BOB)
                .tags(bobTags)
                .build());
        print("Principal tags set", ALICE + "=engineering, " + BOB + "=marketing");

        GetRLSPrincipalTagsResp tagsResp = client.getRLSPrincipalTags(GetRLSPrincipalTagsReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .principalName(ALICE)
                .build());
        print("Alice tags", tagsResp.getTags());

        ListRLSPrincipalsResp principals = client.listRLSPrincipals(ListRLSPrincipalsReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .build());
        print("Principals with tags", principals.getPrincipalNames());

        client.deleteRLSPrincipalTags(DeleteRLSPrincipalTagsReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .principalName(ALICE)
                .tagKeys(Arrays.asList("level"))
                .build());
        print("Alice tag deleted", "level");

        // ---- enforcement: writes are checked against the policy ----
        client.insert(InsertReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row(1L, "engineering")))
                .rlsPrincipal(ALICE)
                .build());
        print("Insert engineering row as alice", "allowed");

        try {
            client.insert(InsertReq.builder()
                    .databaseName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(row(2L, "marketing")))
                    .rlsPrincipal(ALICE)
                    .build());
            print("Insert marketing row as alice", "unexpected success");
        } catch (Exception e) {
            print("Insert marketing row as alice", "rejected by RLS check expression");
        }

        client.insert(InsertReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row(3L, "marketing")))
                .rlsPrincipal(BOB)
                .build());
        print("Insert marketing row as bob", "allowed");

        // load the collection so query can run
        client.createIndex(CreateIndexReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(IndexParam.builder()
                        .fieldName("vector")
                        .indexType(IndexParam.IndexType.FLAT)
                        .metricType(IndexParam.MetricType.L2)
                        .build()))
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .build());

        // ---- enforcement: reads are filtered by the principal's tags ----
        QueryResp aliceRows = client.query(QueryReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .filter("id > 0")
                .rlsPrincipal(ALICE)
                .build());
        print("Query rows as alice", aliceRows.getQueryResults().size());

        QueryResp bobRows = client.query(QueryReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .filter("id > 0")
                .rlsPrincipal(BOB)
                .build());
        print("Query rows as bob", bobRows.getQueryResults().size());

        QueryResp allRows = client.query(QueryReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .filter("id > 0")
                .skipRls(true)
                .build());
        print("Query rows with skipRls", allRows.getQueryResults().size());

        try {
            client.query(QueryReq.builder()
                    .databaseName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .filter("id > 0")
                    .build());
            print("Query without rlsPrincipal", "unexpected success");
        } catch (Exception e) {
            print("Query without rlsPrincipal", "denied: rls_principal is required");
        }

        client.dropRowPolicy(DropRowPolicyReq.builder()
                .databaseName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .policyName(DEMO_POLICY)
                .build());
        print("Row policy dropped", DEMO_POLICY);
    }

    private static JsonObject row(long id, String team) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("id", id);
        jsonObject.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        jsonObject.addProperty("team", team);
        return jsonObject;
    }

    private static void cleanup(MilvusClientV2 client) {
        try {
            client.dropRowPolicy(DropRowPolicyReq.builder()
                    .databaseName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .policyName(DEMO_POLICY)
                    .build());
        } catch (Exception ignored) {
        }
        try {
            client.dropCollection(DropCollectionReq.builder()
                    .databaseName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .build());
        } catch (Exception ignored) {
        }
    }

    private static void print(String message, Object value) {
        System.out.printf("%-40s %s%n", message, value);
    }
}
