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

import io.milvus.v2.service.snapshot.response.PinSnapshotDataResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class PinSnapshotDataRespTest {
    @Test
    void builderBuildsAllFields() {
        PinSnapshotDataResp response = PinSnapshotDataResp.builder()
                .pinId(123L)
                .build();

        assertEquals(Long.valueOf(123L), response.getPinId());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        PinSnapshotDataResp response = PinSnapshotDataResp.builder().build();

        assertNull(response.getPinId());
    }

    @Test
    void setterUpdatesField() {
        PinSnapshotDataResp response = PinSnapshotDataResp.builder().build();

        response.setPinId(456L);

        assertEquals(Long.valueOf(456L), response.getPinId());
    }

    @Test
    void toStringContainsFields() {
        PinSnapshotDataResp response = PinSnapshotDataResp.builder()
                .pinId(1L)
                .build();

        String text = response.toString();
        assertEquals("PinSnapshotDataResp{pinId=1}", text);
    }
}
