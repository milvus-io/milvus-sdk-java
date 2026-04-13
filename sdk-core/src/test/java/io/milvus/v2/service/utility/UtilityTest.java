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

package io.milvus.v2.service.utility;

import com.google.gson.JsonObject;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class UtilityTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UtilityTest.class);

    @Test
    void testCreateAlias() {
        CreateAliasReq req = CreateAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.createAlias(req);
    }

    @Test
    void testDropAlias() {
        DropAliasReq req = DropAliasReq.builder()
                .alias("test_alias")
                .build();
        client_v2.dropAlias(req);
    }

    @Test
    void testAlterAlias() {
        AlterAliasReq req = AlterAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.alterAlias(req);
    }

    @Test
    void describeAlias() {
        DescribeAliasReq req = DescribeAliasReq.builder()
                .alias("test_alias")
                .build();
        DescribeAliasResp statusR = client_v2.describeAlias(req);
    }

    @Test
    void listAliases() {
        ListAliasesReq req = ListAliasesReq.builder()
                .collectionName("test")
                .build();
        ListAliasResp statusR = client_v2.listAliases(req);
    }

    @Test
    void testRefreshExternalCollection() {
        JsonObject spec = new JsonObject();
        spec.addProperty("format", "parquet");
        RefreshExternalCollectionReq req = RefreshExternalCollectionReq.builder()
                .collectionName("ext_coll")
                .externalSource("s3://bucket/path")
                .externalSpec(spec)
                .build();
        RefreshExternalCollectionResp resp = client_v2.refreshExternalCollection(req);
        assertEquals(12345L, resp.getJobId());
    }

    @Test
    void testRefreshExternalCollectionWithDatabase() {
        RefreshExternalCollectionReq req = RefreshExternalCollectionReq.builder()
                .databaseName("my_db")
                .collectionName("ext_coll")
                .build();
        RefreshExternalCollectionResp resp = client_v2.refreshExternalCollection(req);
        assertEquals(12345L, resp.getJobId());
    }

    @Test
    void testGetRefreshExternalCollectionProgress() {
        GetRefreshExternalCollectionProgressReq req = GetRefreshExternalCollectionProgressReq.builder()
                .jobId(12345L)
                .build();
        GetRefreshExternalCollectionProgressResp resp = client_v2.getRefreshExternalCollectionProgress(req);
        RefreshExternalCollectionJobInfo jobInfo = resp.getJobInfo();
        assertNotNull(jobInfo);
        assertEquals(12345L, jobInfo.getJobId());
        assertEquals("ext_coll", jobInfo.getCollectionName());
        assertEquals("RefreshCompleted", jobInfo.getState());
        assertEquals(100, jobInfo.getProgress());
        assertEquals("", jobInfo.getReason());
        assertEquals("s3://bucket/path", jobInfo.getExternalSource());
        assertEquals(1000L, jobInfo.getStartTime());
        assertEquals(2000L, jobInfo.getEndTime());
    }

    @Test
    void testListRefreshExternalCollectionJobs() {
        ListRefreshExternalCollectionJobsReq req = ListRefreshExternalCollectionJobsReq.builder()
                .collectionName("ext_coll")
                .build();
        ListRefreshExternalCollectionJobsResp resp = client_v2.listRefreshExternalCollectionJobs(req);
        assertNotNull(resp.getJobs());
        assertEquals(2, resp.getJobs().size());

        RefreshExternalCollectionJobInfo job1 = resp.getJobs().get(0);
        assertEquals(12345L, job1.getJobId());
        assertEquals("RefreshCompleted", job1.getState());
        assertEquals(100, job1.getProgress());

        RefreshExternalCollectionJobInfo job2 = resp.getJobs().get(1);
        assertEquals(12346L, job2.getJobId());
        assertEquals("RefreshInProgress", job2.getState());
        assertEquals(50, job2.getProgress());
        assertEquals("s3://bucket/path2", job2.getExternalSource());
        assertEquals(0L, job2.getEndTime());
    }

    @Test
    void testListRefreshExternalCollectionJobsWithDatabase() {
        ListRefreshExternalCollectionJobsReq req = ListRefreshExternalCollectionJobsReq.builder()
                .databaseName("my_db")
                .collectionName("ext_coll")
                .build();
        ListRefreshExternalCollectionJobsResp resp = client_v2.listRefreshExternalCollectionJobs(req);
        assertNotNull(resp.getJobs());
        assertEquals(2, resp.getJobs().size());
    }
}