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

package io.milvus.unit.v2.service.snapshot;

import io.milvus.v2.service.snapshot.response.SnapshotSummaryInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class SnapshotSummaryInfoTest {
    @Test
    void builderBuildsAllFields() {
        SnapshotSummaryInfo summary = SnapshotSummaryInfo.builder()
                .name("snap")
                .createTs(9007199254740993L)
                .description("daily backup")
                .createUtcTs(34359738368L)
                .build();

        assertEquals("snap", summary.getName());
        assertEquals(Long.valueOf(9007199254740993L), summary.getCreateTs());
        assertEquals("daily backup", summary.getDescription());
        assertEquals(Long.valueOf(34359738368L), summary.getCreateUtcTs());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        SnapshotSummaryInfo summary = SnapshotSummaryInfo.builder().build();

        assertNull(summary.getName());
        assertNull(summary.getCreateTs());
        assertNull(summary.getDescription());
        assertNull(summary.getCreateUtcTs());
    }

    @Test
    void settersUpdateFields() {
        SnapshotSummaryInfo summary = SnapshotSummaryInfo.builder().build();

        summary.setName("snap2");
        summary.setCreateTs(100L);
        summary.setDescription("desc");
        summary.setCreateUtcTs(50L);

        assertEquals("snap2", summary.getName());
        assertEquals(Long.valueOf(100L), summary.getCreateTs());
        assertEquals("desc", summary.getDescription());
        assertEquals(Long.valueOf(50L), summary.getCreateUtcTs());
    }

    @Test
    void toStringContainsFields() {
        SnapshotSummaryInfo summary = SnapshotSummaryInfo.builder()
                .name("snap")
                .createTs(1000L)
                .description("desc")
                .createUtcTs(200L)
                .build();

        assertEquals("SnapshotSummaryInfo{name='snap', createTs=1000, description='desc', createUtcTs=200}",
                summary.toString());
    }
}
