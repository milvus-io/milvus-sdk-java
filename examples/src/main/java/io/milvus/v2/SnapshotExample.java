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
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.snapshot.request.CreateSnapshotReq;
import io.milvus.v2.service.snapshot.request.DescribeSnapshotReq;
import io.milvus.v2.service.snapshot.request.DropSnapshotReq;
import io.milvus.v2.service.snapshot.request.GetRestoreSnapshotStateReq;
import io.milvus.v2.service.snapshot.request.ListRestoreSnapshotJobsReq;
import io.milvus.v2.service.snapshot.request.ListSnapshotsReq;
import io.milvus.v2.service.snapshot.request.PinSnapshotDataReq;
import io.milvus.v2.service.snapshot.request.RestoreSnapshotReq;
import io.milvus.v2.service.snapshot.request.UnpinSnapshotDataReq;
import io.milvus.v2.service.snapshot.response.DescribeSnapshotResp;
import io.milvus.v2.service.snapshot.response.GetRestoreSnapshotStateResp;
import io.milvus.v2.service.snapshot.response.ListRestoreSnapshotJobsResp;
import io.milvus.v2.service.snapshot.response.ListSnapshotsResp;
import io.milvus.v2.service.snapshot.response.PinSnapshotDataResp;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotJobInfo;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotResp;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SnapshotExample {
    private static final String COLLECTION_NAME = "java_sdk_example_snapshot_v2";
    private static final String RESTORE_COLLECTION_NAME = "java_sdk_example_snapshot_restore_v2";
    private static final String SNAPSHOT_NAME = "java_sdk_example_snapshot_backup";

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        try {
            client.dropCollection(DropCollectionReq.builder().collectionName(RESTORE_COLLECTION_NAME).build());
            client.dropCollection(DropCollectionReq.builder().collectionName(COLLECTION_NAME).build());

            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .dimension(4)
                    .build());
            System.out.printf("Collection '%s' created%n", COLLECTION_NAME);

            List<JsonObject> rows = new ArrayList<>();
            Gson gson = new Gson();
            for (int i = 0; i < 1000; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", i);
                row.add("vector", gson.toJsonTree(new float[]{i, (float) i / 2, (float) i / 3, (float) i / 4}));
                rows.add(row);
            }
            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            System.out.printf("%d rows inserted%n", insertResp.getInsertCnt());

            QueryResp countR = client.query(QueryReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.printf("%d rows persisted\n", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));


            client.flush(FlushReq.builder()
                    .collectionNames(Collections.singletonList(COLLECTION_NAME))
                    .build());
            System.out.println("Collection flushed");

            client.createSnapshot(CreateSnapshotReq.builder()
                    .snapshotName(SNAPSHOT_NAME)
                    .collectionName(COLLECTION_NAME)
                    .description("Snapshot example backup")
                    .build());
            System.out.printf("Snapshot '%s' created%n", SNAPSHOT_NAME);

            ListSnapshotsResp listResp = client.listSnapshots(ListSnapshotsReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.println("Snapshots: " + listResp.getSnapshots());

            DescribeSnapshotResp describeResp = client.describeSnapshot(DescribeSnapshotReq.builder()
                    .snapshotName(SNAPSHOT_NAME)
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.printf("Snapshot detail: name=%s, collection=%s, partitions=%s, createTs=%d, s3Location=%s%n",
                    describeResp.getName(), describeResp.getCollectionName(), describeResp.getPartitionNames(),
                    describeResp.getCreateTs(), describeResp.getS3Location());

            PinSnapshotDataResp pinResp = client.pinSnapshotData(PinSnapshotDataReq.builder()
                    .snapshotName(SNAPSHOT_NAME)
                    .collectionName(COLLECTION_NAME)
                    .ttlSeconds(3600L)
                    .build());
            System.out.printf("Snapshot data pinned, pinId=%d%n", pinResp.getPinId());

            RestoreSnapshotResp restoreResp = client.restoreSnapshot(RestoreSnapshotReq.builder()
                    .snapshotName(SNAPSHOT_NAME)
                    .sourceCollectionName(COLLECTION_NAME)
                    .targetCollectionName(RESTORE_COLLECTION_NAME)
                    .build());
            System.out.printf("Restore snapshot job submitted, jobId=%d%n", restoreResp.getJobId());

            RestoreSnapshotJobInfo jobInfo = waitRestoreSnapshot(client, restoreResp.getJobId());
            System.out.printf("Restore job state: state=%s, progress=%d, reason=%s%n",
                    jobInfo.getState(), jobInfo.getProgress(), jobInfo.getReason());

            long sourceRowCount = getRowCount(client, COLLECTION_NAME);
            long targetRowCount = getRowCount(client, RESTORE_COLLECTION_NAME);
            System.out.printf("Source row count=%d, target row count=%d%n", sourceRowCount, targetRowCount);
            if (sourceRowCount != targetRowCount) {
                throw new RuntimeException(String.format("Restored row count mismatch: source=%d, target=%d",
                        sourceRowCount, targetRowCount));
            }

            client.unpinSnapshotData(UnpinSnapshotDataReq.builder()
                    .pinId(pinResp.getPinId())
                    .build());
            System.out.printf("Snapshot data unpinned, pinId=%d%n", pinResp.getPinId());

            ListRestoreSnapshotJobsResp jobsResp = client.listRestoreSnapshotJobs(ListRestoreSnapshotJobsReq.builder()
                    .collectionName(RESTORE_COLLECTION_NAME)
                    .build());
            System.out.println("Restore snapshot jobs:");
            for (RestoreSnapshotJobInfo job : jobsResp.getJobs()) {
                System.out.printf("  jobId=%d, snapshot=%s, collection=%s, state=%s, progress=%d%n",
                        job.getJobId(), job.getSnapshotName(), job.getCollectionName(), job.getState(), job.getProgress());
            }

            client.dropSnapshot(DropSnapshotReq.builder()
                    .snapshotName(SNAPSHOT_NAME)
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.printf("Snapshot '%s' dropped%n", SNAPSHOT_NAME);
        } finally {
            client.close();
        }
    }

    private static long getRowCount(MilvusClientV2 client, String collectionName) {
        return client.getCollectionStats(GetCollectionStatsReq.builder()
                .collectionName(collectionName)
                .build()).getNumOfEntities();
    }

    private static RestoreSnapshotJobInfo waitRestoreSnapshot(MilvusClientV2 client, Long jobId) throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            GetRestoreSnapshotStateResp stateResp = client.getRestoreSnapshotState(GetRestoreSnapshotStateReq.builder()
                    .jobId(jobId)
                    .build());
            RestoreSnapshotJobInfo jobInfo = stateResp.getJobInfo();
            if (RestoreSnapshotJobInfo.STATE_COMPLETED.equals(jobInfo.getState())) {
                return jobInfo;
            }
            if (RestoreSnapshotJobInfo.STATE_FAILED.equals(jobInfo.getState())) {
                throw new RuntimeException("Restore snapshot failed: " + jobInfo.getReason());
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Restore snapshot did not complete within 60 seconds");
    }
}
