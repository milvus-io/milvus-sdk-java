package com.zilliz.milvustestv2.resourceGroup;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.ListResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class ListResourceGroupTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        ResourceGroupLimit resourceGroupLimit = new ResourceGroupLimit(2);
        ResourceGroupLimit resourceGroupRequest = new ResourceGroupLimit(1);
        milvusClientV2.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .config(ResourceGroupConfig.newBuilder()
                        .withLimits(resourceGroupLimit)
                        .withRequests(resourceGroupRequest)
                        .build()).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        ResourceGroupConfig resourceGroupConfig = ResourceGroupConfig.newBuilder().withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0)).build();
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(CommonData.resourceGroup, resourceGroupConfig);
        milvusClientV2.updateResourceGroups(UpdateResourceGroupsReq.builder().resourceGroups(resourceGroups).build());
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .build());
    }

    @Test(description = "List resource group ", groups = {"Smoke"})
    public void listResourceGroup() {
        ListResourceGroupsResp listResourceGroupsResp = milvusClientV2.listResourceGroups(ListResourceGroupsReq.builder().build());
        System.out.println(listResourceGroupsResp.getGroupNames());
        Assert.assertEquals(listResourceGroupsResp.getGroupNames().size(),2);
        Assert.assertTrue(listResourceGroupsResp.getGroupNames().contains(CommonData.defaultResourceGroup));
    }
}
