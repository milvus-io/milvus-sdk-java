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

public class DescribeResourceGroupTest extends BaseTest {

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

    @Test(description = "", groups = {"Smoke"})
    public void describeResourceGroup() {
        DescribeResourceGroupResp describeResourceGroupResp =
                milvusClientV2.describeResourceGroup(DescribeResourceGroupReq.builder()
                        .groupName(CommonData.resourceGroup)
                        .build());
        Assert.assertEquals(describeResourceGroupResp.getNumberOfAvailableNode(),1);
        Assert.assertEquals(describeResourceGroupResp.getConfig().getRequests().getNodeNum(),1);
        Assert.assertNotNull(describeResourceGroupResp.getNodes().get(0).getAddress());
    }


}
