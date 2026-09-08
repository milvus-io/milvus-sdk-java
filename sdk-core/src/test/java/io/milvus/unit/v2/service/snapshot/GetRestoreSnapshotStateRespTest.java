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

import io.milvus.v2.service.snapshot.response.GetRestoreSnapshotStateResp;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotJobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetRestoreSnapshotStateRespTest {
    @Test
    void builderBuildsAllFields() {
        RestoreSnapshotJobInfo jobInfo = RestoreSnapshotJobInfo.builder().jobId(1L).build();

        GetRestoreSnapshotStateResp response = GetRestoreSnapshotStateResp.builder()
                .jobInfo(jobInfo)
                .build();

        assertSame(jobInfo, response.getJobInfo());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        GetRestoreSnapshotStateResp response = GetRestoreSnapshotStateResp.builder().build();

        assertNull(response.getJobInfo());
    }

    @Test
    void setterUpdatesField() {
        GetRestoreSnapshotStateResp response = GetRestoreSnapshotStateResp.builder().build();

        RestoreSnapshotJobInfo jobInfo = RestoreSnapshotJobInfo.builder().jobId(2L).build();
        response.setJobInfo(jobInfo);

        assertSame(jobInfo, response.getJobInfo());
    }

    @Test
    void toStringContainsFields() {
        GetRestoreSnapshotStateResp response = GetRestoreSnapshotStateResp.builder()
                .jobInfo(RestoreSnapshotJobInfo.builder().jobId(1L).build())
                .build();

        String text = response.toString();
        assertTrue(text.startsWith("GetRestoreSnapshotStateResp{jobInfo=RestoreSnapshotJobInfo{jobId=1"));
    }
}
