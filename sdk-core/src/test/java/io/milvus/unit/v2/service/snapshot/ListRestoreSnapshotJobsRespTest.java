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

import io.milvus.v2.service.snapshot.response.ListRestoreSnapshotJobsResp;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotJobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListRestoreSnapshotJobsRespTest {
    @Test
    void builderBuildsAllFields() {
        List<RestoreSnapshotJobInfo> jobs = List.of(RestoreSnapshotJobInfo.builder().jobId(1L).build());

        ListRestoreSnapshotJobsResp response = ListRestoreSnapshotJobsResp.builder()
                .jobs(jobs)
                .build();

        assertSame(jobs, response.getJobs());
    }

    @Test
    void unsetFieldDefaultsToEmptyList() {
        ListRestoreSnapshotJobsResp response = ListRestoreSnapshotJobsResp.builder().build();

        assertEquals(0, response.getJobs().size());
    }

    @Test
    void nullJobsNormalizedToEmptyList() {
        ListRestoreSnapshotJobsResp response = ListRestoreSnapshotJobsResp.builder()
                .jobs(null)
                .build();

        assertEquals(0, response.getJobs().size());
    }

    @Test
    void setterUpdatesField() {
        ListRestoreSnapshotJobsResp response = ListRestoreSnapshotJobsResp.builder().build();

        List<RestoreSnapshotJobInfo> jobs = List.of(RestoreSnapshotJobInfo.builder().jobId(2L).build());
        response.setJobs(jobs);

        assertSame(jobs, response.getJobs());
    }

    @Test
    void toStringContainsFields() {
        ListRestoreSnapshotJobsResp response = ListRestoreSnapshotJobsResp.builder()
                .jobs(List.of(RestoreSnapshotJobInfo.builder().jobId(1L).build()))
                .build();

        String text = response.toString();
        assertTrue(text.startsWith("ListRestoreSnapshotJobsResp{jobs=[RestoreSnapshotJobInfo{jobId=1"));
    }
}
