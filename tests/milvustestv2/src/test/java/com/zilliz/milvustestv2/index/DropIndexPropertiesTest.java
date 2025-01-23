package com.zilliz.milvustestv2.index;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexPropertiesReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class DropIndexPropertiesTest extends BaseTest {
    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTest() {
        collectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndex(collectionName, DataType.FloatVector);
        List<String> strings = milvusClientV2.listIndexes(ListIndexesReq.builder()
                .collectionName(collectionName).build());
        milvusClientV2.alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName(collectionName)
                .indexName(strings.get(0))
                .property(Constant.MMAP_ENABLED, "true").build());
    }

    @AfterClass(alwaysRun = true)
    public void destroyTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName).build());
    }

    @Test(description = "drop index properties", groups = {"Smoke"})
    public void dropIndexProperties() {
        List<String> strings = milvusClientV2.listIndexes(ListIndexesReq.builder()
                .collectionName(collectionName).build());
        milvusClientV2.dropIndexProperties(DropIndexPropertiesReq.builder()
                .indexName(strings.get(0))
                .propertyKeys(Lists.newArrayList(Constant.MMAP_ENABLED))
                .collectionName(collectionName)
                .build());
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collectionName)
                .indexName(strings.get(0)).build());
        Assert.assertNull(describeIndexResp.getIndexDescriptions().get(0).getProperties().get(Constant.MMAP_ENABLED));
    }
}
