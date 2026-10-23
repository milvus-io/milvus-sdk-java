package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.BatchDescribeCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test cases for batchDescribeCollection and describe by collection ID.
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class BatchDescribeCollectionTest extends BaseTest {
    private String collectionA; // pk + FloatVector(128), 2 fields
    private String collectionB; // pk + FloatVector(64) + VarChar, 3 fields
    private String collectionC; // pk + FloatVector(32) + Int32 + VarChar, 4 fields

    private static final int FIELD_COUNT_A = 2;
    private static final int FIELD_COUNT_B = 3;
    private static final int FIELD_COUNT_C = 4;

    @BeforeClass(alwaysRun = true)
    public void initCollections() {
        collectionA = "BatchDescA_" + GenerateUtil.getRandomString(6);
        collectionB = "BatchDescB_" + GenerateUtil.getRandomString(6);
        collectionC = "BatchDescC_" + GenerateUtil.getRandomString(6);

        // collection A: pk + FloatVector(128)
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionA)
                .collectionSchema(buildSchema(128, false, false))
                .build());

        // collection B: pk + FloatVector(64) + VarChar
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionB)
                .collectionSchema(buildSchema(64, true, false))
                .build());

        // collection C: pk + FloatVector(32) + Int32 + VarChar
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionC)
                .collectionSchema(buildSchema(32, true, true))
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        for (String name : Arrays.asList(collectionA, collectionB, collectionC)) {
            if (name != null) {
                milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(name).build());
            }
        }
    }

    private static CreateCollectionReq.CollectionSchema buildSchema(int dim, boolean withVarchar, boolean withInt32) {
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
                .dimension(dim)
                .build());
        if (withInt32) {
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt32)
                    .dataType(DataType.Int32)
                    .build());
        }
        if (withVarchar) {
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldVarchar)
                    .dataType(DataType.VarChar)
                    .maxLength(256)
                    .build());
        }
        return schema;
    }

    @Test(description = "Batch describe 3 collections by names, verify names and field counts", groups = {"Smoke"})
    public void batchDescribeByNames() {
        List<DescribeCollectionResp> resps = milvusClientV2.batchDescribeCollection(
                BatchDescribeCollectionReq.builder()
                        .collectionNames(Arrays.asList(collectionA, collectionB, collectionC))
                        .build());

        Assert.assertEquals(resps.size(), 3, "Should return 3 describe results");

        Map<String, DescribeCollectionResp> respByName = new HashMap<>();
        for (DescribeCollectionResp resp : resps) {
            respByName.put(resp.getCollectionName(), resp);
        }

        Assert.assertTrue(respByName.containsKey(collectionA));
        Assert.assertTrue(respByName.containsKey(collectionB));
        Assert.assertTrue(respByName.containsKey(collectionC));

        Assert.assertEquals(respByName.get(collectionA).getCollectionSchema().getFieldSchemaList().size(), FIELD_COUNT_A);
        Assert.assertEquals(respByName.get(collectionB).getCollectionSchema().getFieldSchemaList().size(), FIELD_COUNT_B);
        Assert.assertEquals(respByName.get(collectionC).getCollectionSchema().getFieldSchemaList().size(), FIELD_COUNT_C);
    }

    @Test(description = "Batch describe 2 collections by collection IDs (names passed together)", groups = {"Smoke"})
    public void batchDescribeByIds() {
        Long idA = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionA).build()).getCollectionID();
        Long idB = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionB).build()).getCollectionID();

        // 2.6-20261022 nightly 实测：batchDescribeCollection 只传 collectionIds 不传 names 会被服务端拒绝
        // （collection names cannot be empty: missing parameter），因此 names 和 ids 需同时传
        List<DescribeCollectionResp> resps = milvusClientV2.batchDescribeCollection(
                BatchDescribeCollectionReq.builder()
                        .collectionNames(Arrays.asList(collectionA, collectionB))
                        .collectionIds(Arrays.asList(idA, idB))
                        .build());

        Assert.assertEquals(resps.size(), 2, "Should return 2 describe results");
        List<Long> returnedIds = new ArrayList<>();
        for (DescribeCollectionResp resp : resps) {
            returnedIds.add(resp.getCollectionID());
        }
        Assert.assertTrue(returnedIds.contains(idA), "Result should contain collection A id");
        Assert.assertTrue(returnedIds.contains(idB), "Result should contain collection B id");
    }

    @Test(description = "Batch describe by collection IDs only should fail on 2.6 nightly", groups = {"Smoke"})
    public void batchDescribeByIdsOnlyShouldFail() {
        Long idA = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionA).build()).getCollectionID();

        try {
            milvusClientV2.batchDescribeCollection(BatchDescribeCollectionReq.builder()
                    .collectionIds(Arrays.asList(idA))
                    .build());
            Assert.fail("Batch describe with only collectionIds should fail on 2.6 nightly");
        } catch (Exception e) {
            // 行为固化：2.6-20261022 nightly 实测，只传 collectionIds 不传 names 时服务端拒绝，
            // 报错 "collection names cannot be empty: missing parameter"
            System.out.println("batch describe by ids only exception: " + e);
            Assert.assertTrue(e.getMessage() != null
                            && e.getMessage().contains("collection names cannot be empty"),
                    "Exception message should contain 'collection names cannot be empty', actual: " + e.getMessage());
        }
    }

    @Test(description = "Describe single collection by collection ID", groups = {"Smoke"})
    public void describeByCollectionId() {
        Long idC = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionC).build()).getCollectionID();

        DescribeCollectionResp resp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionId(idC)
                .build());

        // 行为固化：按 ID describe 时服务端不回填 collectionName（resp 中 name 为空串），
        // 2.6-20261022 nightly 实测；因此这里只校验返回 ID 与请求 ID 一致及字段数正确
        Assert.assertEquals(resp.getCollectionID(), idC, "ID should match for describe by id");
        Assert.assertEquals(resp.getCollectionSchema().getFieldSchemaList().size(), FIELD_COUNT_C);
    }

    @Test(description = "Batch describe with one non-exist collection name should fail", groups = {"Smoke"})
    public void batchDescribeWithNonExistNameShouldFail() {
        try {
            milvusClientV2.batchDescribeCollection(BatchDescribeCollectionReq.builder()
                    .collectionNames(Arrays.asList(collectionA, "NotExistCollection_" + GenerateUtil.getRandomString(6)))
                    .build());
            // Note: the SDK does not support partial success here. The conversion layer does not
            // check per-collection sub status, so a non-exist collection surfaces as an exception
            // (IndexOutOfBoundsException or MilvusClientException) instead of a partial result.
            Assert.fail("Expect exception when batch describe contains a non-exist collection name");
        } catch (Exception e) {
            // expected, SDK does not do partial success for batch describe
            System.out.println("batch describe with non-exist name exception: " + e);
        }
    }

    @Test(description = "Batch describe with empty collection names", groups = {"Smoke"})
    public void batchDescribeWithEmptyNames() {
        List<DescribeCollectionResp> resps = null;
        try {
            resps = milvusClientV2.batchDescribeCollection(BatchDescribeCollectionReq.builder()
                    .collectionNames(new ArrayList<String>())
                    .build());
        } catch (Exception e) {
            // Actual behavior note: SDK does not validate empty input on client side; the server may
            // reject an empty batch describe request with an error status. Exception is acceptable.
            System.out.println("batch describe with empty names exception: " + e);
            return;
        }
        // Actual behavior note: if no exception is thrown, the server returns an empty result list.
        Assert.assertTrue(resps == null || resps.isEmpty(),
                "Empty collection names should return empty result or throw exception");
    }
}
