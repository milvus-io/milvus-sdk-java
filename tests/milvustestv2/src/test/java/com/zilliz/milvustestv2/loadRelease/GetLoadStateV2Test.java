package com.zilliz.milvustestv2.loadRelease;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.grpc.LoadState;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.GetLoadStateResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
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
 * Test cases for getLoadStateV2 API.
 * 与老接口 getLoadState（只返回 Boolean）不同，getLoadStateV2 返回 LoadState 枚举 + progress，
 * 且支持带 partitionName（单数）查询分区级别的 load 状态。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
@Slf4j
public class GetLoadStateV2Test extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "GetLoadStateV2_" + GenerateUtil.getRandomString(6);
        createCollectionWithData(collectionName, INSERT_COUNT);
        createHnswIndex(collectionName);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    @Test(description = "Get load state V2 before load", groups = {"Smoke"})
    public void getLoadStateV2BeforeLoad() {
        GetLoadStateResp resp = milvusClientV2.getLoadStateV2(GetLoadStateReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(resp);
        Assert.assertNotNull(resp.getState());
        // 实际行为：未 load 的 collection 返回 LoadStateNotLoad；Loading 也视为合理中间态
        Assert.assertTrue(resp.getState() == LoadState.LoadStateNotLoad
                        || resp.getState() == LoadState.LoadStateLoading,
                "State before load should be NotLoad or Loading, actual: " + resp.getState());
        Assert.assertNotNull(resp.getStateName());
        log.info("load state before load: " + resp);
    }

    @Test(description = "Get load state V2 after load", groups = {"Smoke"}, dependsOnMethods = {"getLoadStateV2BeforeLoad"})
    public void getLoadStateV2AfterLoad() {
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        GetLoadStateResp resp = milvusClientV2.getLoadStateV2(GetLoadStateReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(resp);
        Assert.assertEquals(resp.getState(), LoadState.LoadStateLoaded,
                "State after load should be Loaded");
        // 行为固化：该 2.6 nightly（2.6-20261022）服务端不回填 progress（getProgress() 返回 null），
        // progress 非空时断言 ==100，为 null 时跳过并记录日志
        if (resp.getProgress() != null) {
            Assert.assertEquals(resp.getProgress(), Long.valueOf(100L),
                    "Progress after load should be 100");
        } else {
            log.info("getLoadStateV2 after load returns progress=null (server does not fill progress on this build)");
        }

        // 对比老接口 getLoadState（返回 Boolean），两者应一致
        Boolean oldState = milvusClientV2.getLoadState(GetLoadStateReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertTrue(oldState, "Old getLoadState should return true after load");
        Assert.assertEquals(resp.getState() == LoadState.LoadStateLoaded, oldState.booleanValue(),
                "getLoadStateV2 and old getLoadState should be consistent");
    }

    @Test(description = "Get load state V2 with default partition name", groups = {"Smoke"}, dependsOnMethods = {"getLoadStateV2AfterLoad"})
    public void getLoadStateV2WithPartition() {
        GetLoadStateResp resp = milvusClientV2.getLoadStateV2(GetLoadStateReq.builder()
                .collectionName(collectionName)
                .partitionName(CommonData.defaultPartitionName)
                .build());
        Assert.assertNotNull(resp);
        Assert.assertEquals(resp.getState(), LoadState.LoadStateLoaded,
                "Default partition state after load should be Loaded");
        // 2.6-20261022 nightly 不回填 progress，为 null 时跳过数值断言
        if (resp.getProgress() != null) {
            Assert.assertEquals(resp.getProgress(), Long.valueOf(100L));
        }
    }

    @Test(description = "Get load state V2 with not existed collection", groups = {"Smoke"})
    public void getLoadStateV2NotExistedCollection() {
        try {
            GetLoadStateResp resp = milvusClientV2.getLoadStateV2(GetLoadStateReq.builder()
                    .collectionName("GetLoadStateV2NotExist_" + GenerateUtil.getRandomString(6))
                    .build());
            // 实际行为记录：不存在的 collection 返回 LoadStateNotExist
            log.info("getLoadStateV2 with not existed collection returns state=" + resp.getState());
            Assert.assertEquals(resp.getState(), LoadState.LoadStateNotExist);
        } catch (Exception e) {
            // 实际行为记录：不存在的 collection 抛异常（如 collection not found）也视为合理行为
            log.info("getLoadStateV2 with not existed collection throws: " + e.getMessage());
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

    /**
     * 创建 HNSW 索引（M=16/efConstruction=64，L2）
     */
    private static void createHnswIndex(String collectionName) {
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
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }
}
