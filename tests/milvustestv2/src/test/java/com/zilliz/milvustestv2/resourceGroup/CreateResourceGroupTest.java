package com.zilliz.milvustestv2.resourceGroup;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DescribeResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateResourceGroupTest extends BaseTest {
    String newCollectionName;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(newCollectionName, CommonData.fieldFloatVector, IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "create resource group", groups = {"Smoke"})
    public void createResourceGroup() {
        ResourceGroupLimit resourceGroupLimit = new ResourceGroupLimit(2);
        ResourceGroupLimit resourceGroupRequest = new ResourceGroupLimit(1);
        milvusClientV2.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .config(ResourceGroupConfig.newBuilder()
                        .withLimits(resourceGroupLimit)
                        .withRequests(resourceGroupRequest)
                        .build()).build());
        DescribeResourceGroupResp describeResourceGroupResp = milvusClientV2.describeResourceGroup(DescribeResourceGroupReq.builder().groupName(CommonData.resourceGroup).build());
        Assert.assertEquals(describeResourceGroupResp.getNumberOfAvailableNode(),1);
        Assert.assertEquals(describeResourceGroupResp.getConfig().getRequests().getNodeNum(),1);
        Assert.assertNotNull(describeResourceGroupResp.getNodes().get(0).getAddress());
        // clean data
        ResourceGroupConfig resourceGroupConfig = ResourceGroupConfig.newBuilder().withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0)).build();
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(CommonData.resourceGroup, resourceGroupConfig);
        milvusClientV2.updateResourceGroups(UpdateResourceGroupsReq.builder().resourceGroups(resourceGroups).build());
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .build());
    }

    @Test(description = "create resource group exceeds maximum resource", groups = {"L1"})
    public void createResourceGroupExceedMax() {
        ResourceGroupLimit resourceGroupLimit = new ResourceGroupLimit(20);
        ResourceGroupLimit resourceGroupRequest = new ResourceGroupLimit(10);
        milvusClientV2.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup2)
                .config(ResourceGroupConfig.newBuilder()
                        .withLimits(resourceGroupLimit)
                        .withRequests(resourceGroupRequest)
                        .build()).build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .resourceGroups(Lists.newArrayList(CommonData.resourceGroup2))
                .build());
        List<BaseVector> baseVectors = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(newCollectionName)
                .topK(CommonData.topK)
                .annsField(CommonData.fieldFloatVector)
                .outputFields(Lists.newArrayList(CommonData.fieldInt32))
                .data(baseVectors)
                .build());
        Assert.assertEquals(search.getSearchResults().get(0).size(), CommonData.topK);
        // release collection + drop resourceGroup
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        ResourceGroupConfig resourceGroupConfig = ResourceGroupConfig.newBuilder().withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0)).build();
        Map<String, ResourceGroupConfig> resourceGroups2 = new HashMap<>();
        resourceGroups2.put(CommonData.resourceGroup2, resourceGroupConfig);
        milvusClientV2.updateResourceGroups(UpdateResourceGroupsReq.builder().resourceGroups(resourceGroups2).build());
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup2)
                .build());
    }
}
