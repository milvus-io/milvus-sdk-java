package com.zilliz.milvustestv2.others;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.utility.request.GetCompactionPlansReq;
import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import io.milvus.v2.service.utility.response.CompactResp;
import io.milvus.v2.service.utility.response.GetCompactionPlansResp;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;
import io.milvus.v2.service.vector.request.InsertReq;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for compact (force merge) with targetSize / targetSizeUnit / isL0 params (Milvus 3.0)。
 * targetSize null/0 表示服务端默认；负数抛 INVALID_PARAMS；unit 支持 B/KB/MB/GB/TB/PB，
 * 结果取整 MB，小于 1MB 抛 "target_size too small"。
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class ForceMergeTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int BATCH_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "ForceMerge_" + GenerateUtil.getRandomString(6);
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

        // 分两批 insert 且每批后 flush，制造多 segment
        for (int batch = 0; batch < 2; batch++) {
            List<JsonObject> data = new ArrayList<>();
            List<List<Float>> vectors = GenerateUtil.generateFloatVector(BATCH_COUNT, 6, DIM);
            for (int i = 0; i < BATCH_COUNT; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) (batch * BATCH_COUNT + i));
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
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    @Test(description = "Compact with targetSize=1024MB and wait completed", groups = {"Smoke", "V3"})
    public void compactWithTargetSize() throws InterruptedException {
        // 服务端下限 configMaxSize=1024MB，targetSize 小于它会报
        // "targetSize xxx MB should be greater than or equal to configMaxSize 1024 MB"，故取 1024
        CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                .collectionName(collectionName)
                .targetSize(1024L)
                .build());
        Long compactionID = compact.getCompactionID();
        Assert.assertNotNull(compactionID);
        log.info("compact with targetSize=1024MB, compactionID=" + compactionID);

        // 轮询 getCompactionState 到 Completed（180s 超时）
        waitCompactionCompleted(compactionID, 180 * 1000L);

        // getCompactionPlans 记录 plan 信息
        GetCompactionPlansResp plansResp = milvusClientV2.getCompactionPlans(GetCompactionPlansReq.builder()
                .compactionID(compactionID)
                .build());
        Assert.assertNotNull(plansResp);
        log.info("compaction plans: state=" + plansResp.getState() + ", plans=" + plansResp.getPlans());
    }

    @Test(description = "Compact L0 segments, behavior recorded", groups = {"Smoke", "V3"})
    public void compactL0() {
        // 行为记录：L0 compaction 需要存在 L0 segment（删除/upsert 产生），当前数据未产生 L0，
        // 服务端可能返回空 ID 或报错，此处只记录实际行为
        try {
            CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                    .collectionName(collectionName)
                    .isL0(true)
                    .build());
            log.info("compact isL0=true returned compactionID=" + compact.getCompactionID());
        } catch (Exception e) {
            log.info("compact isL0=true throws: " + e.getMessage());
        }
    }

    @Test(description = "Legacy compact without targetSize (server default)", groups = {"Smoke", "V3"})
    public void compactLegacyDefault() {
        // 老接口对照：不带 targetSize，走服务端默认行为
        CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(compact.getCompactionID());
        log.info("legacy compact compactionID=" + compact.getCompactionID());
    }

    @Test(description = "Compact with negative targetSize should throw client side", groups = {"Smoke", "V3"})
    public void compactWithNegativeTargetSize() {
        try {
            milvusClientV2.compact(CompactReq.builder()
                    .collectionName(collectionName)
                    .targetSize(-1L)
                    .build());
            Assert.fail("compact with negative targetSize should throw INVALID_PARAMS");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Compact with targetSize set but targetSizeUnit null should throw", groups = {"Smoke", "V3"})
    public void compactWithNullTargetSizeUnit() {
        try {
            milvusClientV2.compact(CompactReq.builder()
                    .collectionName(collectionName)
                    .targetSize(10L)
                    .targetSizeUnit(null)
                    .build());
            Assert.fail("compact with targetSize set and targetSizeUnit null should throw INVALID_PARAMS");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Compact with targetSize smaller than 1MB should throw", groups = {"Smoke", "V3"})
    public void compactWithTooSmallTargetSize() {
        // 512KB 取整 MB 后小于 1MB，客户端抛 "target_size too small"（以实际报错为准，注释记录）
        try {
            milvusClientV2.compact(CompactReq.builder()
                    .collectionName(collectionName)
                    .targetSize(512L)
                    .targetSizeUnit("kb")
                    .build());
            Assert.fail("compact with targetSize 512KB should throw (smaller than 1MB)");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    /**
     * 轮询 compaction 状态直到 Completed
     */
    private static void waitCompactionCompleted(Long compactionID, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            GetCompactionStateResp stateResp = milvusClientV2.getCompactionState(GetCompactionStateReq.builder()
                    .compactionID(compactionID)
                    .build());
            log.info("compaction " + compactionID + " state=" + stateResp.getState()
                    + ", executingPlanNo=" + stateResp.getExecutingPlanNo()
                    + ", completedPlanNo=" + stateResp.getCompletedPlanNo()
                    + ", timeoutPlanNo=" + stateResp.getTimeoutPlanNo());
            if (stateResp.getState() == CompactionState.Completed) {
                return;
            }
            Thread.sleep(2000L);
        }
        Assert.fail("compaction " + compactionID + " not completed within " + timeoutMs + "ms");
    }
}
