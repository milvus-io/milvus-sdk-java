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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class ListSnapshotsRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> snapshots = Arrays.asList("s1", "s2");

        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(snapshots)
                .build();

        assertSame(snapshots, response.getSnapshots());
    }

    @Test
    void unsetFieldDefaultsToEmptyList() {
        ListSnapshotsResp response = ListSnapshotsResp.builder().build();

        assertEquals(0, response.getSnapshots().size());
    }

    @Test
    void nullSnapshotsNormalizedToEmptyList() {
        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(null)
                .build();

        assertEquals(0, response.getSnapshots().size());
    }

    @Test
    void setterUpdatesField() {
        ListSnapshotsResp response = ListSnapshotsResp.builder().build();

        response.setSnapshots(Arrays.asList("s1"));

        assertEquals(Arrays.asList("s1"), response.getSnapshots());
    }

    @Test
    void toStringContainsFields() {
        ListSnapshotsResp response = ListSnapshotsResp.builder()
                .snapshots(Arrays.asList("s1"))
                .build();

        String text = response.toString();
        assertEquals("ListSnapshotsResp{snapshots=[s1]}", text);
    }
}
