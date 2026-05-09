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

package io.milvus.v2.service.snapshot;

import io.milvus.v2.BaseTest;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.snapshot.request.*;
import io.milvus.v2.service.snapshot.response.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotServiceTest extends BaseTest {
    @Test
    void testCreateSnapshot() {
        CreateSnapshotReq req = CreateSnapshotReq.builder()
                .snapshotName("snapshot_1")
                .collectionName("test")
                .description("test snapshot")
                .compactionProtectionSeconds(60L)
                .build();
        client_v2.createSnapshot(req);
    }

    @Test
    void testDropSnapshot() {
        DropSnapshotReq req = DropSnapshotReq.builder()
                .snapshotName("snapshot_1")
                .collectionName("test")
                .build();
        client_v2.dropSnapshot(req);
    }

    @Test
    void testListSnapshots() {
        ListSnapshotsReq req = ListSnapshotsReq.builder()
                .collectionName("test")
                .build();
        ListSnapshotsResp resp = client_v2.listSnapshots(req);
        assertNotNull(resp.getSnapshots());
        assertEquals(2, resp.getSnapshots().size());
        assertEquals("snapshot_1", resp.getSnapshots().get(0));
        assertEquals("snapshot_2", resp.getSnapshots().get(1));
    }

    @Test
    void testListSnapshotsRespDefaultSnapshots() {
        ListSnapshotsResp resp = ListSnapshotsResp.builder().build();
        assertNotNull(resp.getSnapshots());
        assertTrue(resp.getSnapshots().isEmpty());
    }

    @Test
    void testDescribeSnapshot() {
        DescribeSnapshotReq req = DescribeSnapshotReq.builder()
                .snapshotName("snapshot_1")
                .collectionName("test")
                .build();
        DescribeSnapshotResp resp = client_v2.describeSnapshot(req);
        assertEquals("snapshot_1", resp.getName());
        assertEquals("test snapshot", resp.getDescription());
        assertEquals("test", resp.getCollectionName());
        assertEquals(1, resp.getPartitionNames().size());
        assertEquals("_default", resp.getPartitionNames().get(0));
        assertEquals(1000L, resp.getCreateTs());
        assertEquals("s3://bucket/snapshot_1", resp.getS3Location());
    }

    @Test
    void testRestoreSnapshot() {
        RestoreSnapshotReq req = RestoreSnapshotReq.builder()
                .snapshotName("snapshot_1")
                .sourceCollectionName("test")
                .targetCollectionName("restored_collection")
                .build();
        RestoreSnapshotResp resp = client_v2.restoreSnapshot(req);
        assertEquals(12345L, resp.getJobId());
    }

    @Test
    void testGetRestoreSnapshotState() {
        GetRestoreSnapshotStateReq req = GetRestoreSnapshotStateReq.builder()
                .jobId(12345L)
                .build();
        GetRestoreSnapshotStateResp resp = client_v2.getRestoreSnapshotState(req);
        RestoreSnapshotJobInfo info = resp.getJobInfo();
        assertEquals(12345L, info.getJobId());
        assertEquals("snapshot_1", info.getSnapshotName());
        assertEquals("default", info.getDbName());
        assertEquals("restored_collection", info.getCollectionName());
        assertEquals(RestoreSnapshotJobInfo.STATE_COMPLETED, info.getState());
        assertEquals(100, info.getProgress());
        assertEquals(1000L, info.getStartTime());
        assertEquals(2000L, info.getTimeCost());
    }

    @Test
    void testListRestoreSnapshotJobs() {
        ListRestoreSnapshotJobsReq req = ListRestoreSnapshotJobsReq.builder()
                .collectionName("restored_collection")
                .build();
        ListRestoreSnapshotJobsResp resp = client_v2.listRestoreSnapshotJobs(req);
        assertNotNull(resp.getJobs());
        assertEquals(2, resp.getJobs().size());
        assertEquals(RestoreSnapshotJobInfo.STATE_COMPLETED, resp.getJobs().get(0).getState());
        assertEquals(RestoreSnapshotJobInfo.STATE_EXECUTING, resp.getJobs().get(1).getState());
        assertEquals(50, resp.getJobs().get(1).getProgress());
    }

    @Test
    void testListRestoreSnapshotJobsRespDefaultJobs() {
        ListRestoreSnapshotJobsResp resp = ListRestoreSnapshotJobsResp.builder().build();
        assertNotNull(resp.getJobs());
        assertTrue(resp.getJobs().isEmpty());
    }

    @Test
    void testPinSnapshotData() {
        PinSnapshotDataReq req = PinSnapshotDataReq.builder()
                .snapshotName("snapshot_1")
                .collectionName("test")
                .ttlSeconds(3600L)
                .build();
        PinSnapshotDataResp resp = client_v2.pinSnapshotData(req);
        assertEquals(54321L, resp.getPinId());
    }

    @Test
    void testUnpinSnapshotData() {
        UnpinSnapshotDataReq req = UnpinSnapshotDataReq.builder()
                .pinId(54321L)
                .build();
        client_v2.unpinSnapshotData(req);
    }

    @Test
    void testSnapshotValidation() {
        assertThrows(MilvusClientException.class, () -> client_v2.createSnapshot(CreateSnapshotReq.builder()
                .collectionName("test")
                .snapshotName("snapshot_1")
                .compactionProtectionSeconds(-1L)
                .build()));
        assertThrows(MilvusClientException.class, () -> client_v2.dropSnapshot(DropSnapshotReq.builder()
                .collectionName("test")
                .build()));
        assertThrows(MilvusClientException.class, () -> client_v2.getRestoreSnapshotState(GetRestoreSnapshotStateReq.builder()
                .jobId(0L)
                .build()));
        assertThrows(MilvusClientException.class, () -> client_v2.pinSnapshotData(PinSnapshotDataReq.builder()
                .snapshotName("snapshot_1")
                .collectionName("test")
                .ttlSeconds(-1L)
                .build()));
        assertThrows(MilvusClientException.class, () -> client_v2.unpinSnapshotData(UnpinSnapshotDataReq.builder()
                .pinId(0L)
                .build()));
    }
}
