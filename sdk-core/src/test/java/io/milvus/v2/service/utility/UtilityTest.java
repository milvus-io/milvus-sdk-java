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
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.v2.BaseTest;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilityTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UtilityTest.class);

    @BeforeEach
    @AfterEach
    void clearTimestampCache() {
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void testCreateAlias() {
        CollectionTsCache.getInstance().set("", "default", "test", 100L);
        CreateAliasReq req = CreateAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.createAlias(req);
        assertEquals(100L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
    }

    @Test
    void testDropAlias() {
        CollectionTsCache.getInstance().set("", "default", "test_alias", 100L);
        DropAliasReq req = DropAliasReq.builder()
                .alias("test_alias")
                .build();
        client_v2.dropAlias(req);
        assertEquals(0L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
    }

    @Test
    void testAlterAlias() {
        CollectionTsCache.getInstance().set("", "default", "test", 100L);
        CollectionTsCache.getInstance().set("", "default", "test_alias", 50L);
        AlterAliasReq req = AlterAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.alterAlias(req);
        assertEquals(100L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
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
        assertEquals("test", statusR.getCollectionName());
        assertEquals("default", statusR.getDbName());
        assertTrue(statusR.getAlias().contains("test_alias"));
    }

    @Test
    void getCompactionPlans() {
        GetCompactionPlansReq req = GetCompactionPlansReq.builder()
                .compactionID(123L)
                .build();
        GetCompactionPlansResp resp = client_v2.getCompactionPlans(req);
        assertEquals(123L, resp.getCompactionId());
        assertEquals(CompactionState.Executing, resp.getState());
        assertEquals(1, resp.getPlans().size());
        assertEquals(1L, resp.getPlans().get(0).getTarget());
        assertTrue(resp.getPlans().get(0).getSources().contains(2L));
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
        assertEquals("{\"format\":\"parquet\"}", jobInfo.getExternalSpec());
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

    @Test
    void testAddFileResource() {
        AddFileResourceReq req = AddFileResourceReq.builder()
                .name("test_resource")
                .path("/data/test.parquet")
                .build();
        client_v2.addFileResource(req);
    }

    @Test
    void testRemoveFileResource() {
        RemoveFileResourceReq req = RemoveFileResourceReq.builder()
                .name("test_resource")
                .build();
        client_v2.removeFileResource(req);
    }

    @Test
    void testListFileResources() {
        ListFileResourcesReq req = ListFileResourcesReq.builder().build();
        ListFileResourcesResp resp = client_v2.listFileResources(req);
        assertNotNull(resp.getResources());
        assertEquals(2, resp.getResources().size());

        FileResourceInfo info1 = resp.getResources().get(0);
        assertEquals("test_resource", info1.getName());
        assertEquals("/data/test.parquet", info1.getPath());

        FileResourceInfo info2 = resp.getResources().get(1);
        assertEquals("test_resource_2", info2.getName());
        assertEquals("/data/test2.parquet", info2.getPath());
    }

    @Test
    void testGetPersistentSegmentInfo() {
        GetPersistentSegmentInfoResp resp = client_v2.getPersistentSegmentInfo(GetPersistentSegmentInfoReq.builder()
                .collectionName("test")
                .build());

        assertEquals(1, resp.getSegmentInfos().size());
        GetPersistentSegmentInfoResp.PersistentSegmentInfo info = resp.getSegmentInfos().get(0);
        assertEquals(1L, info.getSegmentID());
        assertEquals(2L, info.getCollectionID());
        assertEquals(3L, info.getPartitionID());
        assertEquals("test", info.getCollectionName());
        assertEquals(4L, info.getNumOfRows());
        assertEquals("Flushed", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(5L, info.getStorageVersion());
        assertTrue(info.getIsSorted());
    }

    @Test
    void testGetQuerySegmentInfo() {
        GetQuerySegmentInfoResp resp = client_v2.getQuerySegmentInfo(GetQuerySegmentInfoReq.builder()
                .collectionName("test")
                .build());

        assertEquals(1, resp.getSegmentInfos().size());
        GetQuerySegmentInfoResp.QuerySegmentInfo info = resp.getSegmentInfos().get(0);
        assertEquals("test", info.getCollectionName());
        assertEquals(6L, info.getSegmentID());
        assertEquals(7L, info.getCollectionID());
        assertEquals(8L, info.getPartitionID());
        assertEquals(9L, info.getMemSize());
        assertEquals(10L, info.getNumOfRows());
        assertEquals("test_index", info.getIndexName());
        assertEquals(11L, info.getIndexID());
        assertEquals("Sealed", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(1, info.getNodeIDs().size());
        assertEquals(12L, info.getNodeIDs().get(0));
        assertEquals(13L, info.getStorageVersion());
        assertTrue(info.getIsSorted());
    }
}
