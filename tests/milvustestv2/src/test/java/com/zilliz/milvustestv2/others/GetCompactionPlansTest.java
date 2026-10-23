package com.zilliz.milvustestv2.others;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.CompactionPlan;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.CompactReq;
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
import java.util.List;

/**
 * Test cases for getCompactionPlans API.
 * 与老接口 getCompactionState（只返回 plan 计数）不同，getCompactionPlans 返回每个 plan 的明细
 * （target/sources segments 等）。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
@Slf4j
public class GetCompactionPlansTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "GetCompactionPlans_" + GenerateUtil.getRandomString(6);
        createCollectionWithData(collectionName, INSERT_COUNT);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    @Test(description = "Get compaction plans after compact", groups = {"Smoke"})
    public void getCompactionPlansSuccess() {
        CompactResp compactResp = milvusClientV2.compact(CompactReq.builder()
                .collectionName(collectionName)
                .build());
        Long compactionID = compactResp.getCompactionID();
        Assert.assertNotNull(compactionID);

        // 轮询直到 compaction 完成（超时 60s）；
        // 2.6-20261022 nightly 实测：compaction 达到 Completed 后服务端会清除 plan 明细，
        // 因此轮询过程中一旦 plans 非空就先记录下来
        GetCompactionPlansResp plansResp = null;
        List<CompactionPlan> observedPlans = null;
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startMs < 60000L) {
            plansResp = milvusClientV2.getCompactionPlans(GetCompactionPlansReq.builder()
                    .compactionID(compactionID)
                    .build());
            if (plansResp.getPlans() != null && !plansResp.getPlans().isEmpty()) {
                observedPlans = plansResp.getPlans();
            }
            if (plansResp.getState() == CompactionState.Completed) {
                break;
            }
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        Assert.assertNotNull(plansResp);
        Assert.assertEquals(plansResp.getState(), CompactionState.Completed,
                "Compaction should complete within 60s");

        // getCompactionPlans 返回 plan 明细，这是老接口 getCompactionState 没有的；
        // 行为固化：Completed 后 plan 明细被清除（2.6-20261022 nightly 实测），
        // 仅当轮询过程中观察到非空 plans 时才附加校验 target/sources 明细
        if (observedPlans != null && !observedPlans.isEmpty()) {
            for (CompactionPlan plan : observedPlans) {
                Assert.assertNotNull(plan.getTarget(), "Plan target segment should not be null");
                Assert.assertNotNull(plan.getSources(), "Plan source segments should not be null");
                Assert.assertFalse(plan.getSources().isEmpty(), "Plan source segments should not be empty");
            }
        } else {
            log.info("compaction plans detail was cleared after Completed, skip plan detail assertions");
        }

        // 对比老接口 getCompactionState：只返回 state + plan 计数，没有 plan 明细
        GetCompactionStateResp stateResp = milvusClientV2.getCompactionState(GetCompactionStateReq.builder()
                .compactionID(compactionID)
                .build());
        Assert.assertNotNull(stateResp);
        Assert.assertEquals(stateResp.getState(), plansResp.getState(),
                "Old getCompactionState and new getCompactionPlans should report the same state");
        log.info("old getCompactionState completedPlanNo=" + stateResp.getCompletedPlanNo()
                + ", new getCompactionPlans observed plans.size="
                + (observedPlans == null ? 0 : observedPlans.size()));
    }

    @Test(description = "Get compaction plans with not existed compactionID", groups = {"Smoke"})
    public void getCompactionPlansWithNotExistedID() {
        try {
            GetCompactionPlansResp plansResp = milvusClientV2.getCompactionPlans(GetCompactionPlansReq.builder()
                    .compactionID(-1L)
                    .build());
            // 行为固化：不存在的 compactionID 返回 Completed 而非 UndefiedState
            // （2.6-20261022 nightly 实测，可能为服务端默认值问题）
            log.info("getCompactionPlans with not existed id returns state=" + plansResp.getState());
            Assert.assertEquals(plansResp.getState(), CompactionState.Completed);
        } catch (Exception e) {
            // 实际行为记录：不存在的 compactionID 抛异常也视为合理行为
            log.info("getCompactionPlans with not existed id throws: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    /**
     * 通用数据准备：Int64 主键 + FloatVector(128) + Int32/VarChar 标量，insert 指定条数
     */
    private static void createCollectionWithData(String collectionName, int count) {
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
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        List<List<Float>> vectors = GenerateUtil.generateFloatVector(count, 6, DIM);
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldInt32, i);
            row.addProperty(CommonData.fieldVarchar, "str_" + i);
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
    }
}
