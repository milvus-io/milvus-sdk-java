package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.grpc.CollectionSchema;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AlterCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 11:11
 */
public class DescribeCollectionTest extends BaseTest {

    @Test(description = "Describe collection", groups = {"Smoke"})
    public void describeCollection() {
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .build());
        Assert.assertEquals(describeCollectionResp.getVectorFieldNames().get(0), CommonData.fieldFloatVector);
    }

    @Test(description = "Describe collection when mmap field", groups = {"Smoke"})
    public void describeMMapCollection() {
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        Map<String, String> map = new HashMap<String, String>() {{
            put(Constant.MMAP_ENABLED, "true");
        }};
        milvusClientV2.alterCollection(AlterCollectionReq.builder()
                .collectionName(newCollection)
                .properties(map).build());
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newCollection)
                .build());
        Assert.assertTrue(describeCollectionResp.getProperties().containsKey(Constant.MMAP_ENABLED));
        Assert.assertTrue(describeCollectionResp.getProperties().get(Constant.MMAP_ENABLED).equalsIgnoreCase("true"));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

    @Test(description = "Describe collection with nullable fields", groups = {"Smoke"})
    public void describeNullableCollection() {
        String newCollection = CommonFunction.createNewNullableCollection(CommonData.dim, null, DataType.FloatVector);
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newCollection)
                .build());
        List<String> fieldNames = describeCollectionResp.getFieldNames();
        // except for the first field (pk) and the last field(vector), other fields should have nullable property
        for(int i = 1; i < fieldNames.size()-1; i++){
            Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(fieldNames.get(i)).getIsNullable());
        }
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

    @Test(description = "Describe collection with default value fields", groups = {"Smoke"})
    public void describeDefaultValueCollection() {
        String newCollection = CommonFunction.createNewDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newCollection)
                .build());
        // except for the first field (pk), json, array, and the last field(vector), other fields should have default value property
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt32).getDefaultValue().equals(CommonData.defaultValueInt));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt16).getDefaultValue().equals(CommonData.defaultValueShort));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt8).getDefaultValue().equals(CommonData.defaultValueShort));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldDouble).getDefaultValue().equals(CommonData.defaultValueDouble));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldFloat).getDefaultValue().equals(CommonData.defaultValueFloat));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldVarchar).getDefaultValue().equals(CommonData.defaultValueString));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldBool).getDefaultValue().equals(CommonData.defaultValueBool));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

    @Test(description = "Describe collection with both nullable and default value enable fields", groups = {"Smoke"})
    public void describeNullDefaultValueCollection() {
        String newCollection = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newCollection)
                .build());
        List<String> fieldNames = describeCollectionResp.getFieldNames();
        // except for the first field (pk) and the last field(vector), other fields should have nullable property
        for(int i = 1; i < fieldNames.size()-1; i++){
            Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(fieldNames.get(i)).getIsNullable());
        }
        // except for the first field (pk), json, array, and the last field(vector), other fields should have nullable property
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt32).getDefaultValue().equals(CommonData.defaultValueInt));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt16).getDefaultValue().equals(CommonData.defaultValueShort));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldInt8).getDefaultValue().equals(CommonData.defaultValueShort));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldDouble).getDefaultValue().equals(CommonData.defaultValueDouble));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldFloat).getDefaultValue().equals(CommonData.defaultValueFloat));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldVarchar).getDefaultValue().equals(CommonData.defaultValueString));
        Assert.assertTrue(describeCollectionResp.getCollectionSchema().getField(CommonData.fieldBool).getDefaultValue().equals(CommonData.defaultValueBool));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

}
