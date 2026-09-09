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

package io.milvus.unit.v2.service.utility;

import io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RefreshExternalCollectionJobInfoTest {
    @Test
    void builderBuildsAllFields() {
        RefreshExternalCollectionJobInfo info = RefreshExternalCollectionJobInfo.builder()
                .jobId(1L)
                .collectionName("coll")
                .state("Running")
                .progress(50)
                .reason("none")
                .externalSource("s3")
                .externalSpec("{}")
                .startTime(100L)
                .endTime(200L)
                .build();

        assertEquals(1L, info.getJobId());
        assertEquals("coll", info.getCollectionName());
        assertEquals("Running", info.getState());
        assertEquals(50, info.getProgress());
        assertEquals("none", info.getReason());
        assertEquals("s3", info.getExternalSource());
        assertEquals("{}", info.getExternalSpec());
        assertEquals(100L, info.getStartTime());
        assertEquals(200L, info.getEndTime());
    }

    @Test
    void unsetFieldsUseDefaults() {
        RefreshExternalCollectionJobInfo info = RefreshExternalCollectionJobInfo.builder().build();

        assertEquals(0L, info.getJobId());
        assertNull(info.getCollectionName());
        assertNull(info.getState());
        assertEquals(0, info.getProgress());
        assertNull(info.getReason());
        assertNull(info.getExternalSource());
        assertNull(info.getExternalSpec());
        assertEquals(0L, info.getStartTime());
        assertEquals(0L, info.getEndTime());
    }

    @Test
    void progressBoundaryValues() {
        assertEquals(0, RefreshExternalCollectionJobInfo.builder().progress(0).build().getProgress());
        assertEquals(100, RefreshExternalCollectionJobInfo.builder().progress(100).build().getProgress());
        assertEquals(-1, RefreshExternalCollectionJobInfo.builder().progress(-1).build().getProgress());
    }

    @Test
    void toStringContainsFields() {
        RefreshExternalCollectionJobInfo info = RefreshExternalCollectionJobInfo.builder()
                .jobId(1L)
                .collectionName("coll")
                .state("Running")
                .progress(50)
                .reason("none")
                .externalSource("s3")
                .externalSpec("{}")
                .startTime(100L)
                .endTime(200L)
                .build();

        String text = info.toString();
        assertEquals("RefreshExternalCollectionJobInfo{jobId=1, collectionName='coll', state='Running', progress=50, reason='none', externalSource='s3', externalSpec='{}', startTime=100, endTime=200}", text);
    }
}
