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
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.OptimizeTask;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.utility.request.GetQuerySegmentInfoReq;
import io.milvus.v2.service.utility.request.OptimizeReq;
import io.milvus.v2.service.utility.response.GetQuerySegmentInfoResp;
import io.milvus.v2.service.utility.response.OptimizeResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;

import java.util.*;

public class OptimizeExample {
    private static final String COLLECTION_NAME = "java_sdk_example_optimize_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIM = 512;
    private static final int TOTAL_ROWS = 1_000_000;
    private static final int BATCH_SIZE = 10_000;

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println(client.getServerVersion());

        // Step 1: Drop and create collection
        System.out.println("========== Step 1: Create collection ==========");
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .build());
        System.out.printf("Collection '%s' created%n", COLLECTION_NAME);

        // Step 2: Insert one million rows, size is 2GB when dimension is 512
        System.out.println("========== Step 2: Insert 1,000,000 rows ==========");
        Gson gson = new Gson();
        int totalInserted = 0;
        for (int batch = 0; batch < TOTAL_ROWS / BATCH_SIZE; batch++) {
            List<JsonObject> rows = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                JsonObject row = new JsonObject();
                row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
                rows.add(row);
            }
            InsertResp resp = client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            totalInserted += (int) resp.getInsertCnt();
            if ((batch + 1) % 10 == 0) {
                System.out.printf("  Inserted %d / %d rows%n", totalInserted, TOTAL_ROWS);
            }
        }
        client.flush(FlushReq.builder().collectionNames(Collections.singletonList(COLLECTION_NAME)).build());
        System.out.printf("Total inserted: %d rows%n", totalInserted);

        // Step 3: Create IVF_FLAT index
        System.out.println("========== Step 3: Create IVF_FLAT index ==========");
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 32);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(indexParam))
                .timeout(100000L)
                .build());
        System.out.println("IVF_FLAT index created");

        // Step 4: Load collection
        System.out.println("========== Step 4: Load collection ==========");
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection loaded");

        // Step 5: Check segments before optimize
        System.out.println("========== Step 5: Query segment info (before optimize) ==========");
        printSegmentInfo(client);

        // Step 6: Optimize with targetSize=4GB, synchronous
        // Data will be merged into one segment because total size is 2GB, which is smaller than targetSize.
        // In standalone Milvus, performance will be the best if data is merged into one segment.
        // But in cluster Milvus, it's recommended to have multiple segments for better load balancing and query performance,
        // so you need to carefully set targetSize based on your data size and cluster configuration.
        System.out.println("========== Step 6: Optimize (targetSize=4GB, sync) ==========");
        long startTime = System.currentTimeMillis();
        OptimizeTask task = client.optimize(OptimizeReq.builder()
                .collectionName(COLLECTION_NAME)
                .targetSize("4GB")
                .build());
        OptimizeResp result = task.getResult(null);
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("Optimize completed in %.1f seconds%n", elapsed / 1000.0);
        System.out.printf("  Status: %s%n", result.getStatus());
        System.out.printf("  Compaction ID: %d%n", result.getCompactionId());
        System.out.printf("  Progress: %s%n", result.getProgress());

        // Step 8: Check segments after optimize
        System.out.println("========== Step 8: Query segment info (after optimize) ==========");
        while (true) {
            int segmentCount = printSegmentInfo(client);
            if (segmentCount == 1) {
                System.out.println("Optimization successful, only one segment remains");
                break;
            }
            System.out.println("Waiting for optimization to complete...");
            Thread.sleep(1000);
        }

        client.close(5);
    }

    private static int printSegmentInfo(MilvusClientV2 client) {
        GetQuerySegmentInfoResp segResp = client.getQuerySegmentInfo(
                GetQuerySegmentInfoReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());
        List<GetQuerySegmentInfoResp.QuerySegmentInfo> segments = segResp.getSegmentInfos();
        System.out.printf("  Total segments: %d%n", segments.size());
        long totalRows = 0;
        for (GetQuerySegmentInfoResp.QuerySegmentInfo seg : segments) {
            System.out.printf("    Segment %d: rows=%d, state=%s, level=%s%n",
                    seg.getSegmentID(), seg.getNumOfRows(), seg.getState(), seg.getLevel());
            totalRows += seg.getNumOfRows();
        }
        System.out.printf("  Total rows across segments: %d%n", totalRows);
        return segments.size();
    }
}
