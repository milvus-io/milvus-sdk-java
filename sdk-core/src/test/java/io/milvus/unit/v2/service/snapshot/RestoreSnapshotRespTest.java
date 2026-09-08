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

import io.milvus.v2.service.snapshot.response.RestoreSnapshotResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RestoreSnapshotRespTest {
    @Test
    void builderBuildsAllFields() {
        RestoreSnapshotResp response = RestoreSnapshotResp.builder()
                .jobId(123L)
                .build();

        assertEquals(Long.valueOf(123L), response.getJobId());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        RestoreSnapshotResp response = RestoreSnapshotResp.builder().build();

        assertNull(response.getJobId());
    }

    @Test
    void setterUpdatesField() {
        RestoreSnapshotResp response = RestoreSnapshotResp.builder().build();

        response.setJobId(456L);

        assertEquals(Long.valueOf(456L), response.getJobId());
    }

    @Test
    void toStringContainsFields() {
        RestoreSnapshotResp response = RestoreSnapshotResp.builder()
                .jobId(1L)
                .build();

        String text = response.toString();
        assertEquals("RestoreSnapshotResp{jobId=1}", text);
    }
}
