package com.zilliz.milvustestv2.loadRelease;

import com.google.gson.JsonObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.common.resourcegroup.ResourceGroupTransfer;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author yongpeng.li
 * @Date 2024/2/23 14:59
 */
public class LoadCollectionTest extends BaseTest {
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

    @Test(description = "Load collection", groups = {"Smoke"})
    public void loadCollection() {
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
        SearchResp searchResp = CommonFunction.defaultSearch(newCollectionName);
        Assert.assertEquals(searchResp.getSearchResults().size(), CommonData.topK);
    }

    @Test(description = "Load collection with partial field", groups = {"Smoke"})
    public void loadCollectionWithPartialField() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .loadFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldInt32, CommonData.fieldFloatVector))
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
    }

    @Test(description = "search when the field not  loaded", groups = {"Smoke"})
    public void searchWhenTheFieldNotLoaded() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .loadFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldInt32, CommonData.fieldFloatVector))
                .build());
        List<BaseVector> baseVectors = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        try {
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(newCollectionName)
                    .topK(CommonData.topK)
                    .annsField(CommonData.fieldFloatVector)
                    .outputFields(Lists.newArrayList(CommonData.fieldInt16))
                    .data(baseVectors)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not loaded"));
        }
    }

    @Test(description = "Load need contains pk field", groups = {"Smoke"})
    public void loadNeedContainsPrimaryKey() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        try {
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(newCollectionName)
                    .loadFields(Lists.newArrayList(CommonData.fieldInt32, CommonData.fieldFloatVector))
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("does not contain primary key field"));
        }
    }

    @Test(description = "Load need contains vector field", groups = {"Smoke"})
    public void loadNeedContainsVectorField() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        try {
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(newCollectionName)
                    .loadFields(Lists.newArrayList(CommonData.fieldInt32, CommonData.fieldInt64))
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("does not contain vector field"));
        }
    }

    @Test(description = "Load with resource group", groups = {"Smoke"})
    public void loadWithResourceGroup() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        // 创建resource group
        ResourceGroupLimit resourceGroupLimit = new ResourceGroupLimit(2);
        ResourceGroupLimit resourceGroupRequest = new ResourceGroupLimit(1);
        milvusClientV2.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .config(ResourceGroupConfig.newBuilder()
                        .withLimits(resourceGroupLimit)
                        .withRequests(resourceGroupRequest)
                        .build()).build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .resourceGroups(Lists.newArrayList(CommonData.resourceGroup))
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
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(CommonData.resourceGroup, resourceGroupConfig);
        milvusClientV2.updateResourceGroups(UpdateResourceGroupsReq.builder().resourceGroups(resourceGroups).build());
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .build());
        milvusClientV2.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(CommonData.resourceGroup)
                .build());
    }

    @Test(description = "Load with replica nul", groups = {"Cluster","L1"})
    public void loadWithReplicaNum() {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .numReplicas(1)
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
    }


}
