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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.CancelRequestsReq;
import io.milvus.v2.service.utility.request.ListRunningRequestsReq;
import io.milvus.v2.service.utility.response.CancelRequestsResp;
import io.milvus.v2.service.utility.response.ListRunningRequestsResp;
import io.milvus.v2.service.utility.response.RunningRequestInfo;
import io.milvus.v2.service.utility.response.RunningRequestNodeResult;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

// List the Search, HybridSearch and Query requests running in a cluster, and cancel one by id.
// The example starts a deliberately heavy search (many query vectors, brute-force FLAT index)
// in a background thread, finds it with listRunningRequests, cancels it with cancelRequests,
// and shows that the search's own client fails with error code 3002 (request canceled).
//
// Requires a Milvus server that supports ListRunningRequests / CancelRequests (Milvus 3.0+).
// With authorization enabled, listing and canceling other users' requests requires the
// ListRunningRequests / CancelRequests privileges; any user may list and cancel their own.
// MILVUS_URI and MILVUS_TOKEN override the default connection settings.
public class RunningRequestsExample {
    private static final String URI = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
    private static final String TOKEN = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");
    private static final String COLLECTION_NAME = "java_sdk_example_running_requests";
    private static final int DIM = 128;
    private static final int ROW_COUNT = 100000;
    private static final int NQ = Integer.parseInt(System.getenv().getOrDefault("EXAMPLE_NQ", "5000"));

    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(URI)
                .token(TOKEN)
                .build());
    }

    private static List<Float> randomVector(Random random) {
        List<Float> vector = new ArrayList<>(DIM);
        for (int i = 0; i < DIM; i++) {
            vector.add(random.nextFloat());
        }
        return vector;
    }

    private static void prepareCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // A FLAT index makes every search a brute-force scan, so a search with many query vectors
        // runs long enough to be seen and canceled. The index parameters take effect only together
        // with an explicit schema: without one, createCollection builds its own AUTOINDEX.
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        IndexParam flat = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build();
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(Collections.singletonList(flat))
                .build());

        Gson gson = new Gson();
        Random random = new Random(42);
        int batch = 5000;
        for (int start = 0; start < ROW_COUNT; start += batch) {
            List<JsonObject> rows = new ArrayList<>();
            for (int i = start; i < start + batch; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", i);
                row.add("vector", gson.toJsonTree(randomVector(random)));
                rows.add(row);
            }
            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
        }
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("Collection '%s' has %d rows\n", COLLECTION_NAME,
                (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void printNodeResults(List<RunningRequestNodeResult> nodeResults) {
        for (RunningRequestNodeResult node : nodeResults) {
            if (node.isSuccess()) {
                System.out.printf("  proxy %d answered\n", node.getNodeId());
            } else {
                System.out.printf("  proxy %d did not answer (unimplemented=%s): %s\n",
                        node.getNodeId(), node.getUnimplemented(), node.getReason());
            }
        }
    }

    private static void listAll() {
        ListRunningRequestsResp resp = client.listRunningRequests(ListRunningRequestsReq.builder().build());
        System.out.printf("\n%d request(s) running in the cluster\n", resp.getRequests().size());
        printNodeResults(resp.getNodeResults());
    }

    private static void cancelHeavySearch() throws InterruptedException {
        Random random = new Random(7);
        List<BaseVector> vectors = new ArrayList<>(NQ);
        for (int i = 0; i < NQ; i++) {
            vectors.add(new FloatVec(randomVector(random)));
        }

        AtomicReference<String> outcome = new AtomicReference<>("not finished");
        Thread searcher = new Thread(() -> {
            try {
                client.search(SearchReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .data(vectors)
                        .limit(100)
                        .build());
                outcome.set("the search finished before it was canceled");
            } catch (MilvusClientException e) {
                outcome.set(String.format("the search failed with error code %d: %s",
                        e.getServerErrCode(), e.getMessage()));
            }
        });
        searcher.start();

        // Find the search by its collection, then cancel it by id.
        RunningRequestInfo target = null;
        long deadline = System.currentTimeMillis() + 10000;
        while (target == null && searcher.isAlive() && System.currentTimeMillis() < deadline) {
            ListRunningRequestsResp listed = client.listRunningRequests(ListRunningRequestsReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            for (RunningRequestInfo info : listed.getRequests()) {
                if ("Search".equals(info.getType())) {
                    target = info;
                    break;
                }
            }
            if (target == null) {
                Thread.sleep(20);
            }
        }

        if (target == null) {
            searcher.join();
            System.out.printf("\nThe search was not seen running (%s); raise EXAMPLE_NQ to make it slower\n",
                    outcome.get());
            return;
        }
        System.out.printf("\nFound the search: id=%s user=%s nq=%d state=%s elapsed=%dms\n",
                target.getRequestId(), target.getUser(), target.getNq(), target.getState(), target.getElapsedMs());

        CancelRequestsResp canceled = client.cancelRequests(CancelRequestsReq.builder()
                .requestIds(Collections.singletonList(target.getRequestId()))
                .reason("canceled by RunningRequestsExample")
                .build());
        for (RunningRequestInfo info : canceled.getCanceled()) {
            System.out.printf("Canceled %s after %dms (state before the cancel: %s)\n",
                    info.getRequestId(), info.getElapsedMs(), info.getState());
        }
        System.out.println("Not found: " + canceled.getNotFound());
        System.out.println("Undetermined: " + canceled.getUndetermined());

        searcher.join();
        System.out.println("Search client: " + outcome.get());
    }

    private static void cancelUnknownId() {
        // An id that no proxy holds is reported in notFound once every proxy has answered.
        CancelRequestsResp resp = client.cancelRequests(CancelRequestsReq.builder()
                .requestIds(Collections.singletonList("0-0-1"))
                .build());
        System.out.println("\nCancel an unknown id, not found: " + resp.getNotFound());
    }

    public static void main(String[] args) throws InterruptedException {
        prepareCollection();
        listAll();
        cancelHeavySearch();
        cancelUnknownId();

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        client.close();
    }
}
