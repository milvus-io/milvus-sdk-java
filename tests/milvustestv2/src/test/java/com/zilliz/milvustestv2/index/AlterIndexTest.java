package com.zilliz.milvustestv2.index;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.AlterIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterIndexTest extends BaseTest {
    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTest() {
        collectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndex(collectionName, DataType.FloatVector);
    }

    @AfterClass(alwaysRun = true)
    public void destroyTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName).build());
    }

    @Test(description = "alter index mmap", groups = {"Smoke"})
    public void alterIndexMMapTest() {
        List<String> strings = milvusClientV2.listIndexes(ListIndexesReq.builder()
                .collectionName(collectionName).build());
        System.out.println(strings);
        Map<String, String> stringMap = new HashMap<>();
        stringMap.put(Constant.MMAP_ENABLED, "true");
        milvusClientV2.alterIndex(AlterIndexReq.builder()
                .indexName(strings.get(0))
                .collectionName(collectionName)
                .properties(stringMap)
                .build());
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collectionName)
                .indexName(strings.get(0)).build());
        System.out.println(describeIndexResp);
        Assert.assertTrue(describeIndexResp.getIndexDescriptions().get(0).getProperties().get(Constant.MMAP_ENABLED).equalsIgnoreCase("true"));

    }


}
