/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.snapshot;

import io.milvus.v2.service.snapshot.response.RestoreSnapshotJobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RestoreSnapshotJobInfoTest {
    @Test
    void stateConstantsAreStable() {
        assertEquals("RestoreSnapshotNone", RestoreSnapshotJobInfo.STATE_NONE);
        assertEquals("RestoreSnapshotPending", RestoreSnapshotJobInfo.STATE_PENDING);
        assertEquals("RestoreSnapshotExecuting", RestoreSnapshotJobInfo.STATE_EXECUTING);
        assertEquals("RestoreSnapshotCompleted", RestoreSnapshotJobInfo.STATE_COMPLETED);
        assertEquals("RestoreSnapshotFailed", RestoreSnapshotJobInfo.STATE_FAILED);
    }

    @Test
    void builderBuildsAllFields() {
        RestoreSnapshotJobInfo info = RestoreSnapshotJobInfo.builder()
                .jobId(1L)
                .snapshotName("snap")
                .dbName("db")
                .collectionName("coll")
                .state(RestoreSnapshotJobInfo.STATE_COMPLETED)
                .progress(100)
                .reason("none")
                .startTime(1000L)
                .timeCost(500L)
                .build();

        assertEquals(Long.valueOf(1L), info.getJobId());
        assertEquals("snap", info.getSnapshotName());
        assertEquals("db", info.getDbName());
        assertEquals("coll", info.getCollectionName());
        assertEquals(RestoreSnapshotJobInfo.STATE_COMPLETED, info.getState());
        assertEquals(Integer.valueOf(100), info.getProgress());
        assertEquals("none", info.getReason());
        assertEquals(Long.valueOf(1000L), info.getStartTime());
        assertEquals(Long.valueOf(500L), info.getTimeCost());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        RestoreSnapshotJobInfo info = RestoreSnapshotJobInfo.builder().build();

        assertNull(info.getJobId());
        assertNull(info.getSnapshotName());
        assertNull(info.getDbName());
        assertNull(info.getCollectionName());
        assertNull(info.getState());
        assertNull(info.getProgress());
        assertNull(info.getReason());
        assertNull(info.getStartTime());
        assertNull(info.getTimeCost());
    }

    @Test
    void settersUpdateFields() {
        RestoreSnapshotJobInfo info = RestoreSnapshotJobInfo.builder().build();

        info.setJobId(2L);
        info.setSnapshotName("snap2");
        info.setDbName("db2");
        info.setCollectionName("coll2");
        info.setState(RestoreSnapshotJobInfo.STATE_FAILED);
        info.setProgress(0);
        info.setReason("boom");
        info.setStartTime(2000L);
        info.setTimeCost(700L);

        assertEquals(Long.valueOf(2L), info.getJobId());
        assertEquals("snap2", info.getSnapshotName());
        assertEquals("db2", info.getDbName());
        assertEquals("coll2", info.getCollectionName());
        assertEquals(RestoreSnapshotJobInfo.STATE_FAILED, info.getState());
        assertEquals(Integer.valueOf(0), info.getProgress());
        assertEquals("boom", info.getReason());
        assertEquals(Long.valueOf(2000L), info.getStartTime());
        assertEquals(Long.valueOf(700L), info.getTimeCost());
    }

    @Test
    void progressAcceptsOutOfRangeValues() {
        assertEquals(Integer.valueOf(-1), RestoreSnapshotJobInfo.builder().progress(-1).build().getProgress());
        assertEquals(Integer.valueOf(101), RestoreSnapshotJobInfo.builder().progress(101).build().getProgress());
    }

    @Test
    void toStringContainsFields() {
        RestoreSnapshotJobInfo info = RestoreSnapshotJobInfo.builder()
                .jobId(1L)
                .snapshotName("snap")
                .dbName("db")
                .collectionName("coll")
                .state(RestoreSnapshotJobInfo.STATE_PENDING)
                .progress(50)
                .reason("r")
                .startTime(100L)
                .timeCost(200L)
                .build();

        String text = info.toString();
        assertEquals("RestoreSnapshotJobInfo{jobId=1, snapshotName='snap', dbName='db', collectionName='coll', state='RestoreSnapshotPending', progress=50, reason='r', startTime=100, timeCost=200}", text);
    }
}
