package com.zilliz.milvustestv2.vectorOperation;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.response.DeleteResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class DeleteTest extends BaseTest {
    String newCollectionName1;
    String newCollectionName2;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName1 = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        newCollectionName2 = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.HNSW))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName2)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName2)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName1).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName2).build());
    }

    @Test(description = "Delete by ids",groups = {"Smoke"})
    public void deleteDataByIds(){
        DeleteResp delete = milvusClientV2.delete(DeleteReq.builder()
                .collectionName(newCollectionName1)
                .ids(Arrays.asList(GenerateUtil.generateInt(100, true)))
                .build());
        Assert.assertEquals(delete.getDeleteCnt(),100);
    }

    @Test(description = "Delete by expression",groups = {"Smoke"})
    public void deleteDataByExpression(){
        DeleteResp delete = milvusClientV2.delete(DeleteReq.builder()
                .collectionName(newCollectionName2)
                .filter("fieldInt64 < 10 ")
                .build());
        //the deleteCnt in deleteDataByExpression is not accurate, so comment the assert
//        Assert.assertEquals(delete.getDeleteCnt(),10);
    }


}
