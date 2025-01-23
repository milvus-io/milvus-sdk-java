package com.zilliz.milvustestv2.resourceGroup;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DescribeResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class UpdateResourceGroupsTest extends BaseTest {
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
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .build());
    }

    @Test(description = "update resource group", groups = {"Smoke"})
    public void updateResourceGroup() {
        ResourceGroupConfig resourceGroupConfig = ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0))
                .build();
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(CommonData.resourceGroup, resourceGroupConfig);
        milvusClientV2.updateResourceGroups(UpdateResourceGroupsReq.builder().resourceGroups(resourceGroups).build());
        DescribeResourceGroupResp describeResourceGroupResp =
                milvusClientV2.describeResourceGroup(DescribeResourceGroupReq.builder()
                        .groupName(CommonData.resourceGroup)
                        .build());
        Assert.assertEquals(describeResourceGroupResp.getNumberOfAvailableNode(),0);
        Assert.assertEquals(describeResourceGroupResp.getConfig().getRequests().getNodeNum(),0);
        Assert.assertEquals(describeResourceGroupResp.getNodes().size(), 0);
    }
}
