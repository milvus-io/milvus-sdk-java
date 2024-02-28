package com.zilliz.milvustestv2.loadRelease;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.LoadPartitionsReq;
import io.milvus.v2.service.partition.request.ReleasePartitionsReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:35
 */
public class ReleasePartitionsTest extends BaseTest {
    String newCollection;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        newCollection = CommonFunction.createNewCollection(128, null);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(newCollection)
                .partitionName(CommonData.partitionName)
                .build());
        List<JSONObject> jsonObjects = CommonFunction.generateDefaultData(CommonData.numberEntities, CommonData.dim);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollection).partitionName(CommonData.partitionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(newCollection,CommonData.fieldFloatVector, IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollection)
                .build());
    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

    @Test(description = "Load partition",groups = {"Smoke"})
    public void releasePartition(){
        milvusClientV2.releasePartitions(ReleasePartitionsReq.builder()
                .collectionName(newCollection)
                .partitionNames(Collections.singletonList(CommonData.partitionName))
                .build());
        try {
            milvusClientV2.search(SearchReq.builder()
                    .collectionName(newCollection)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .vectorFieldName(CommonData.fieldFloatVector)
                    .partitionNames(Lists.newArrayList(CommonData.partitionName))
                    .data(GenerateUtil.generateFloatVector(10, 3, CommonData.dim))
                    .topK(CommonData.topK)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("partition not loaded"));
        }
    }

}
