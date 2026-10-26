package com.zilliz.milvustestv2.others;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.snapshot.request.CreateSnapshotReq;
import io.milvus.v2.service.snapshot.request.DescribeSnapshotReq;
import io.milvus.v2.service.snapshot.request.DropSnapshotReq;
import io.milvus.v2.service.snapshot.request.GetRestoreSnapshotStateReq;
import io.milvus.v2.service.snapshot.request.ListRestoreSnapshotJobsReq;
import io.milvus.v2.service.snapshot.request.ListSnapshotsReq;
import io.milvus.v2.service.snapshot.request.PinSnapshotDataReq;
import io.milvus.v2.service.snapshot.request.RestoreSnapshotReq;
import io.milvus.v2.service.snapshot.request.UnpinSnapshotDataReq;
import io.milvus.v2.service.snapshot.response.DescribeSnapshotResp;
import io.milvus.v2.service.snapshot.response.GetRestoreSnapshotStateResp;
import io.milvus.v2.service.snapshot.response.ListRestoreSnapshotJobsResp;
import io.milvus.v2.service.snapshot.response.ListSnapshotsResp;
import io.milvus.v2.service.snapshot.response.PinSnapshotDataResp;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotJobInfo;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotResp;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Test cases for snapshot APIs (Milvus 3.0): createSnapshot / listSnapshots / describeSnapshot /
 * pinSnapshotData / restoreSnapshot / getRestoreSnapshotState / listRestoreSnapshotJobs /
 * unpinSnapshotData / dropSnapshot.
 * 前置：Milvus 3.x 服务端（snapshot 依赖服务端对象存储，standalone 自带 minio 即可）。
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class SnapshotTest extends BaseTest {

    private String collectionName;
    private String restoreCollectionName;
    private String snapshotName;
    private Long pinId;

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 500;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "Snapshot_" + GenerateUtil.getRandomString(6);
        restoreCollectionName = collectionName + "_restore";
        snapshotName = "snap1";
        // 建 collection（Int64 pk + FloatVector(128)），insert 500 条并 flush
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        List<List<Float>> vectors = GenerateUtil.generateFloatVector(INSERT_COUNT, 6, DIM);
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            JsonArray vector = new JsonArray();
            for (Float f : vectors.get(i)) {
                vector.add(f);
            }
            row.add(CommonData.fieldFloatVector, vector);
            data.add(row);
        }
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        milvusClientV2.flush(FlushReq.builder()
                .collectionNames(Collections.singletonList(collectionName))
                .waitFlushedTimeoutMs(60 * 1000L)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        // 防御式清理：unpin / dropSnapshot / dropCollection
        if (pinId != null) {
            try {
                milvusClientV2.unpinSnapshotData(UnpinSnapshotDataReq.builder().pinId(pinId).build());
            } catch (Exception e) {
                log.warn("cleanup unpinSnapshotData failed: " + e.getMessage());
            }
        }
        if (collectionName != null) {
            try {
                milvusClientV2.dropSnapshot(DropSnapshotReq.builder()
                        .snapshotName(snapshotName)
                        .collectionName(collectionName)
                        .build());
            } catch (Exception e) {
                log.warn("cleanup dropSnapshot failed: " + e.getMessage());
            }
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(restoreCollectionName)
                    .build());
        }
    }

    @Test(description = "Snapshot full lifecycle: create/list/describe/pin/restore/unpin/drop", groups = {"Smoke", "V3"})
    public void snapshotFullLifecycle() throws InterruptedException {
        // 1. createSnapshot
        milvusClientV2.createSnapshot(CreateSnapshotReq.builder()
                .collectionName(collectionName)
                .snapshotName(snapshotName)
                .description("snapshot smoke test")
                .build());

        // 2. listSnapshots 含 snap1
        ListSnapshotsResp listResp = milvusClientV2.listSnapshots(ListSnapshotsReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(listResp.getSnapshots());
        Assert.assertTrue(listResp.getSnapshots().contains(snapshotName),
                "listSnapshots should contain " + snapshotName + ", actual: " + listResp.getSnapshots());

        // 3. describeSnapshot 校验 name/collectionName/createTs 非空
        DescribeSnapshotResp describeResp = milvusClientV2.describeSnapshot(DescribeSnapshotReq.builder()
                .snapshotName(snapshotName)
                .collectionName(collectionName)
                .build());
        Assert.assertEquals(describeResp.getName(), snapshotName);
        Assert.assertEquals(describeResp.getCollectionName(), collectionName);
        Assert.assertNotNull(describeResp.getCreateTs(), "createTs should not be null");
        log.info("describeSnapshot: name=" + describeResp.getName() + ", collection=" + describeResp.getCollectionName()
                + ", partitions=" + describeResp.getPartitionNames() + ", createTs=" + describeResp.getCreateTs()
                + ", s3Location=" + describeResp.getS3Location());

        // 4. pinSnapshotData 拿 pinId
        PinSnapshotDataResp pinResp = milvusClientV2.pinSnapshotData(PinSnapshotDataReq.builder()
                .snapshotName(snapshotName)
                .collectionName(collectionName)
                .ttlSeconds(3600L)
                .build());
        pinId = pinResp.getPinId();
        Assert.assertNotNull(pinId);
        log.info("pinSnapshotData pinId=" + pinId);

        // 5. restoreSnapshot 到新集合（异步）
        RestoreSnapshotResp restoreResp = milvusClientV2.restoreSnapshot(RestoreSnapshotReq.builder()
                .snapshotName(snapshotName)
                .sourceCollectionName(collectionName)
                .targetCollectionName(restoreCollectionName)
                .build());
        Assert.assertNotNull(restoreResp.getJobId());
        log.info("restoreSnapshot jobId=" + restoreResp.getJobId());

        // 6. 轮询 getRestoreSnapshotState 到成功态（60s 超时）
        RestoreSnapshotJobInfo jobInfo = waitRestoreSuccess(restoreResp.getJobId(), 60 * 1000L);
        log.info("restore job done: state=" + jobInfo.getState() + ", progress=" + jobInfo.getProgress()
                + ", reason=" + jobInfo.getReason() + ", timeCost=" + jobInfo.getTimeCost());

        // listRestoreSnapshotJobs 应包含该 job
        ListRestoreSnapshotJobsResp jobsResp = milvusClientV2.listRestoreSnapshotJobs(
                ListRestoreSnapshotJobsReq.builder()
                        .collectionName(restoreCollectionName)
                        .build());
        Assert.assertNotNull(jobsResp.getJobs());
        Assert.assertFalse(jobsResp.getJobs().isEmpty(), "listRestoreSnapshotJobs should not be empty");

        // 7. 新集合建索引 load 后 count(*)==500（数据完整）
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 64);
                }})
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(restoreCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(restoreCollectionName)
                .build());
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(restoreCollectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Long count = (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
        Assert.assertEquals(count, Long.valueOf(INSERT_COUNT), "Restored collection row count mismatch");

        // 8. unpinSnapshotData
        milvusClientV2.unpinSnapshotData(UnpinSnapshotDataReq.builder().pinId(pinId).build());
        pinId = null;

        // 9. dropSnapshot 后 listSnapshots 为空
        milvusClientV2.dropSnapshot(DropSnapshotReq.builder()
                .snapshotName(snapshotName)
                .collectionName(collectionName)
                .build());
        ListSnapshotsResp listAfterDrop = milvusClientV2.listSnapshots(ListSnapshotsReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(listAfterDrop.getSnapshots());
        Assert.assertTrue(listAfterDrop.getSnapshots().isEmpty(),
                "listSnapshots should be empty after dropSnapshot, actual: " + listAfterDrop.getSnapshots());
    }

    @Test(description = "Create snapshot with duplicated name should fail", groups = {"Smoke", "V3"})
    public void createDuplicatedSnapshot() {
        String dupSnapshot = "snap_dup_" + GenerateUtil.getRandomString(6);
        milvusClientV2.createSnapshot(CreateSnapshotReq.builder()
                .collectionName(collectionName)
                .snapshotName(dupSnapshot)
                .description("first one")
                .build());
        try {
            milvusClientV2.createSnapshot(CreateSnapshotReq.builder()
                    .collectionName(collectionName)
                    .snapshotName(dupSnapshot)
                    .description("duplicated one")
                    .build());
            Assert.fail("createSnapshot with duplicated name should fail");
        } catch (Exception e) {
            log.info("expected exception for duplicated snapshot name: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        } finally {
            try {
                milvusClientV2.dropSnapshot(DropSnapshotReq.builder()
                        .snapshotName(dupSnapshot)
                        .collectionName(collectionName)
                        .build());
            } catch (Exception e) {
                log.warn("cleanup dup snapshot failed: " + e.getMessage());
            }
        }
    }

    @Test(description = "Describe not existed snapshot should fail", groups = {"Smoke", "V3"})
    public void describeNotExistedSnapshot() {
        try {
            milvusClientV2.describeSnapshot(DescribeSnapshotReq.builder()
                    .snapshotName("snap_not_exist_" + GenerateUtil.getRandomString(6))
                    .collectionName(collectionName)
                    .build());
            Assert.fail("describeSnapshot on not existed snapshot should fail");
        } catch (Exception e) {
            log.info("expected exception for describe not existed snapshot: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Drop not existed snapshot, behavior recorded", groups = {"Smoke", "V3"})
    public void dropNotExistedSnapshot() {
        // 行为记录：drop 不存在的 snapshot 可能抛异常也可能幂等成功，以实际服务端行为为准
        try {
            milvusClientV2.dropSnapshot(DropSnapshotReq.builder()
                    .snapshotName("snap_not_exist_" + GenerateUtil.getRandomString(6))
                    .collectionName(collectionName)
                    .build());
            log.info("dropSnapshot on not existed snapshot: no exception (idempotent)");
        } catch (Exception e) {
            log.info("dropSnapshot on not existed snapshot throws: " + e.getMessage());
        }
    }

    /**
     * 轮询 restore job 直到成功态；成功判定：state 为 RestoreSnapshotCompleted，
     * 或 state 字符串含 "Completed"/"Success"（忽略大小写），或 progress==100（以实际为准）。
     * 失败态（含 "Failed"）直接 Assert.fail 并记录 reason。
     */
    private static RestoreSnapshotJobInfo waitRestoreSuccess(Long jobId, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            GetRestoreSnapshotStateResp stateResp = milvusClientV2.getRestoreSnapshotState(
                    GetRestoreSnapshotStateReq.builder().jobId(jobId).build());
            RestoreSnapshotJobInfo jobInfo = stateResp.getJobInfo();
            String state = jobInfo.getState() == null ? "" : jobInfo.getState();
            Integer progress = jobInfo.getProgress();
            log.info("restore job " + jobId + " state=" + state + ", progress=" + progress);
            if (state.toLowerCase().contains("failed")) {
                Assert.fail("restore snapshot job failed, reason: " + jobInfo.getReason());
            }
            if (RestoreSnapshotJobInfo.STATE_COMPLETED.equals(state)
                    || state.toLowerCase().contains("completed")
                    || state.toLowerCase().contains("success")
                    || (progress != null && progress == 100)) {
                return jobInfo;
            }
            Thread.sleep(1000L);
        }
        Assert.fail("restore snapshot job " + jobId + " not completed within " + timeoutMs + "ms");
        return null;
    }
}
