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

import io.milvus.v2.service.snapshot.response.ListSnapshotsResp;
import io.milvus.v2.service.snapshot.response.SnapshotSummaryInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class ListSnapshotsRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> snapshots = Arrays.asList("s1", "s2");
        List<SnapshotSummaryInfo> summaries = Arrays.asList(
                SnapshotSummaryInfo.builder().name("s1").createTs(200L).description("d1").createUtcTs(20L).build(),
                SnapshotSummaryInfo.builder().name("s2").createTs(100L).description("d2").createUtcTs(10L).build()
        );

        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(snapshots)
                .snapshotSummaries(summaries)
                .build();

        assertSame(snapshots, response.getSnapshots());
        assertSame(summaries, response.getSnapshotSummaries());
    }

    @Test
    void unsetFieldDefaultsToEmptyList() {
        ListSnapshotsResp response = ListSnapshotsResp.builder().build();

        assertEquals(0, response.getSnapshots().size());
        assertEquals(0, response.getSnapshotSummaries().size());
    }

    @Test
    void nullSnapshotsNormalizedToEmptyList() {
        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(null)
                .snapshotSummaries(null)
                .build();

        assertEquals(0, response.getSnapshots().size());
        assertEquals(0, response.getSnapshotSummaries().size());
    }

    @Test
    void setterUpdatesField() {
        ListSnapshotsResp response = ListSnapshotsResp.builder().build();
        List<SnapshotSummaryInfo> summaries = Collections.singletonList(
                SnapshotSummaryInfo.builder().name("s1").createTs(1L).description("").createUtcTs(0L).build()
        );

        response.setSnapshots(Arrays.asList("s1"));
        response.setSnapshotSummaries(summaries);

        assertEquals(Arrays.asList("s1"), response.getSnapshots());
        assertSame(summaries, response.getSnapshotSummaries());
    }

    @Test
    void toStringContainsFields() {
        SnapshotSummaryInfo summary = SnapshotSummaryInfo.builder()
                .name("s1")
                .createTs(10L)
                .description("desc")
                .createUtcTs(5L)
                .build();
        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(Arrays.asList("s1"))
                .snapshotSummaries(Collections.singletonList(summary))
                .build();

        String text = response.toString();
        assertEquals("ListSnapshotsResp{snapshots=[s1], snapshotSummaries=[" + summary + "]}", text);
    }
}
