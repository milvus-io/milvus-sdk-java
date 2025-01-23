package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AlterCollectionFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AlterCollectionFieldTest extends BaseTest {

    String newCollection;

    @BeforeClass(alwaysRun = true)
    public void intiTestData() {
        newCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }


    @Test(description = "alter collection field", groups = {"Smoke"})
    public void alterCollectionField() {
        milvusClientV2.alterCollectionField(AlterCollectionFieldReq.builder()
                .collectionName(newCollection)
                .fieldName(CommonData.fieldVarchar)
                .property("max_length", "99")
                .build());
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newCollection)
                .build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        CreateCollectionReq.FieldSchema field = collectionSchema.getField(CommonData.fieldVarchar);
        Assert.assertEquals(field.getMaxLength(), 99);
    }
}
