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

import io.milvus.v2.service.utility.response.ListRefreshExternalCollectionJobsResp;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListRefreshExternalCollectionJobsRespTest {
    @Test
    void builderBuildsAllFields() {
        List<RefreshExternalCollectionJobInfo> jobs = Arrays.asList(
                RefreshExternalCollectionJobInfo.builder().jobId(1L).build());

        ListRefreshExternalCollectionJobsResp response = ListRefreshExternalCollectionJobsResp.builder()
                .jobs(jobs)
                .build();

        assertSame(jobs, response.getJobs());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        ListRefreshExternalCollectionJobsResp response = ListRefreshExternalCollectionJobsResp.builder().build();

        assertNull(response.getJobs());
    }

    @Test
    void toStringContainsFields() {
        ListRefreshExternalCollectionJobsResp response = ListRefreshExternalCollectionJobsResp.builder()
                .jobs(Arrays.asList(RefreshExternalCollectionJobInfo.builder().jobId(1L).build()))
                .build();

        String text = response.toString();
        assertTrue(text.startsWith("ListRefreshExternalCollectionJobsResp{jobs=[RefreshExternalCollectionJobInfo{jobId=1"));
    }
}
