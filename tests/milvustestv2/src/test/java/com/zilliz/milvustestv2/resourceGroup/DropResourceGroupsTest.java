package com.zilliz.milvustestv2.resourceGroup;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class DropResourceGroupsTest extends BaseTest {

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

    }

    @Test(description = "drop resource group need set node num=0", groups = {"Smoke"})
    public void dropResourceGroupBeforeChangeNodeNum() {
        try {
            milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                    .groupName(CommonData.resourceGroup)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("num is not 0"));
        }

    }

    @Test(description = "drop resource group", groups = {"Smoke"},dependsOnMethods = {"dropResourceGroupBeforeChangeNodeNum"})
    public void dropResourceGroup() {
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


}
