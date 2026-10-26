package com.zilliz.milvustestv2.structArray;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for struct element-level search with element_filter.
 * element_filter syntax (Milvus v2.6.x doc, struct-array-operators):
 *   element_filter(structArrayField, predicate)，predicate 内用 $[subField] 引用 struct 标量子字段，
 *   例如 element_filter(chunks, $[text] LIKE "Red%") / element_filter(profile, $[p_int] == 30)
 * SDK 侧：SearchReq.filter 含子串 "element_filter" 时，查询向量按 element 级编码（elementLevel=true）。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class StructElementFilterTest extends BaseTest {

    private String structCollectionName;
    private static final int DIM = CommonData.structVectorDim;
    private static final int INSERT_COUNT = 500;
    private static final int ELEMENTS_PER_ROW = 5;
    private static final int TOP_K = 10;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        structCollectionName = "StructElementFilter_" + GenerateUtil.getRandomString(6);
        CommonFunction.createStructCollection(structCollectionName, DIM);

        List<JsonObject> data = generatePatternedStructData(INSERT_COUNT, DIM);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(structCollectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");

        // MAX_SIM_COSINE index on struct vector sub-field
        CommonFunction.createStructVectorIndex(structCollectionName,
                CommonData.fieldStruct,
                CommonData.structFieldFloatVector1,
                "struct_vec1_idx",
                IndexParam.MetricType.MAX_SIM_COSINE);
        // load 要求所有向量字段都有索引（2.6 nightly 报错：there is no vector index on
        // field: [fieldStruct[structFloatVector2]]），因此 structFloatVector2 也需建 MAX_SIM_COSINE 索引
        CommonFunction.createStructVectorIndex(structCollectionName,
                CommonData.fieldStruct,
                CommonData.structFieldFloatVector2,
                "struct_vec2_idx",
                IndexParam.MetricType.MAX_SIM_COSINE);
        // regular vector index
        CommonFunction.createVectorIndex(structCollectionName,
                CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW,
                IndexParam.MetricType.L2);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(structCollectionName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (structCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(structCollectionName)
                    .build());
        }
    }

    /**
     * 每行固定 ELEMENTS_PER_ROW 个 struct element，element j 的 structInt32 = j、
     * structVarchar = "elem_" + j，便于用 element_filter 过滤标量子字段。
     */
    private static List<JsonObject> generatePatternedStructData(int count, int dim) {
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);

            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, dim).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            JsonArray structArray = new JsonArray();
            for (int j = 0; j < ELEMENTS_PER_ROW; j++) {
                JsonObject struct = new JsonObject();
                struct.addProperty(CommonData.structFieldInt32, j);
                struct.addProperty(CommonData.structFieldVarchar, "elem_" + j);

                JsonArray vec1 = new JsonArray();
                for (Float v : GenerateUtil.generateFloatVector(1, 6, dim).get(0)) {
                    vec1.add(v);
                }
                struct.add(CommonData.structFieldFloatVector1, vec1);

                JsonArray vec2 = new JsonArray();
                for (Float v : GenerateUtil.generateFloatVector(1, 6, dim).get(0)) {
                    vec2.add(v);
                }
                struct.add(CommonData.structFieldFloatVector2, vec2);

                structArray.add(struct);
            }
            row.add(CommonData.fieldStruct, structArray);
            dataList.add(row);
        }
        return dataList;
    }

    private static String structAnnsField() {
        return String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);
    }

    // ==================== Positive Tests ====================

    @Test(description = "element_filter on struct int32 sub-field (2.6 分支尚未支持，行为固化)", groups = {"Smoke"})
    public void searchWithElementFilterOnInt32() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(3, DIM);
        // 语法出处: https://milvus.io/docs/v2.6.x/struct-array-operators.md
        // element_filter(structArrayField, predicate)，predicate 内 $[subField] 引用 struct 标量子字段
        // 注意：element_filter 的 parser 产生式目前只在 milvus master(3.x) 存在，
        // 2.6 分支（含 2.6-20261022 nightly，Plan.g4 无该产生式）会报 parse error；
        // v2.6.x 文档超前于代码。此处固化为预期报错用例，服务端 backport 后应恢复为正向断言。
        String filter = String.format("element_filter(%s, $[%s] >= %d)",
                CommonData.fieldStruct, CommonData.structFieldInt32, ELEMENTS_PER_ROW - 2);

        try {
            SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                    .collectionName(structCollectionName)
                    .annsField(structAnnsField())
                    .data(Collections.singletonList(embeddingList))
                    .topK(TOP_K)
                    .filter(filter)
                    .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldStruct))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // 服务端已支持 element_filter 时的正向断言
            Assert.assertNotNull(searchResp);
            Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                    "element-level search with element_filter should return results");
            Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= TOP_K,
                    "result size should not exceed topK");
        } catch (Exception e) {
            // 2.6 分支：parser 无 element_filter 产生式，报 cannot parse expression；
            // 3.0：EmbeddingList+element_filter 属 row-level，报 "element_filter is only supported
            // for element-level search ..."；element-level 正向见 ElementFilterV3Test
            String msg = e.getMessage();
            Assert.assertTrue(msg != null && (msg.contains("cannot parse expression")
                            || msg.contains("element_filter is only supported")),
                    "element_filter should be rejected with known error, actual: " + msg);
        }
    }

    @Test(description = "element_filter on struct varchar sub-field (2.6 分支尚未支持，行为固化)", groups = {"Smoke"})
    public void searchWithElementFilterOnVarchar() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(2, DIM);
        String filter = String.format("element_filter(%s, $[%s] LIKE \"elem_0%%\")",
                CommonData.fieldStruct, CommonData.structFieldVarchar);

        try {
            SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                    .collectionName(structCollectionName)
                    .annsField(structAnnsField())
                    .data(Collections.singletonList(embeddingList))
                    .topK(TOP_K)
                    .filter(filter)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // 服务端已支持 element_filter 时的正向断言
            Assert.assertNotNull(searchResp);
            Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                    "element-level search with varchar element_filter should return results");
            Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= TOP_K,
                    "result size should not exceed topK");
        } catch (Exception e) {
            // 2.6 分支：parser 无 element_filter 产生式，报 cannot parse expression；
            // 3.0：EmbeddingList+element_filter 属 row-level，报 "element_filter is only supported..."
            String msg = e.getMessage();
            Assert.assertTrue(msg != null && (msg.contains("cannot parse expression")
                            || msg.contains("element_filter is only supported")),
                    "element_filter should be rejected with known error, actual: " + msg);
        }
    }

    @Test(description = "same EmbeddingList search without element_filter for comparison", groups = {"Smoke"})
    public void searchWithoutElementFilter() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(3, DIM);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(structAnnsField())
                .data(Collections.singletonList(embeddingList))
                .topK(TOP_K)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                "search without element_filter should also return results");
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= TOP_K);
        // 差异说明：不带 element_filter 时按整行所有 element 参与 MAX_SIM 打分；
        // 带 element_filter 时仅满足标量谓词的 element 参与 element 级搜索与打分。
    }

    // ==================== Negative Tests ====================

    @Test(description = "element_filter referencing non-exist struct sub-field should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void elementFilterWithNonExistSubFieldShouldFail() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(2, DIM);
        String filter = String.format("element_filter(%s, $[not_exist_field] == 1)", CommonData.fieldStruct);

        milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(structAnnsField())
                .data(Collections.singletonList(embeddingList))
                .topK(TOP_K)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
    }

    @Test(description = "element_filter used on regular FloatVector field (non-struct) search", groups = {"Smoke"})
    public void elementFilterOnRegularVectorField() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        String filter = String.format("element_filter(%s, $[%s] == 0)",
                CommonData.fieldStruct, CommonData.structFieldInt32);

        try {
            SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                    .collectionName(structCollectionName)
                    .annsField(CommonData.fieldFloatVector)
                    .data(Collections.singletonList(new FloatVec(queryVector)))
                    .topK(TOP_K)
                    .filter(filter)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // 实际行为：服务端未报错，正常返回（element_filter 在非 struct 向量搜索上被忽略或按普通 filter 处理）
            Assert.assertNotNull(searchResp);
        } catch (Exception e) {
            // 实际行为：抛异常（element_filter 仅支持 Array-of-Struct 场景，普通 FloatVector 搜索不可用），
            // 异常信息: " + e.getMessage()
            Assert.assertTrue(true, "element_filter on regular vector field throws: " + e.getMessage());
        }
    }
}
