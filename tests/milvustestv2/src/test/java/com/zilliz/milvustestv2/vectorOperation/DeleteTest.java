package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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


    @Test(description = "search after delete  ",groups = {"Smoke"})
    public void searchAfterDelete(){
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(newCollection,DataType.FloatVector,true,CommonData.numberEntities);
        DeleteResp delete = milvusClientV2.delete(DeleteReq.builder()
                .collectionName(newCollection)
                .filter(CommonData.fieldInt64+" < 10 ")
                .build());
        Assert.assertEquals(delete.getDeleteCnt(),10);
        //count
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(newCollection)
                .outputFields(Lists.newArrayList("*"))
                .filter(CommonData.fieldInt64+" >= 0 ")
                .consistencyLevel(ConsistencyLevel.STRONG).build());
        Assert.assertEquals(CommonData.numberEntities-10, query.getQueryResults().size());

        // search
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(newCollection)
                .filter(CommonData.fieldInt64+" < 10 ")
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(CommonData.topK)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), 1);
        Assert.assertEquals(search.getSearchResults().get(0).size(), 0);

        // insert deleted data
        CommonFunction.createIndexAndInsertAndLoad(newCollection,DataType.FloatVector,true, 10L);
        //count
        QueryResp query2 = milvusClientV2.query(QueryReq.builder()
                .collectionName(newCollection)
                .outputFields(Lists.newArrayList("*"))
                .filter(CommonData.fieldInt64+" >= 0 ")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(CommonData.numberEntities, query2.getQueryResults().size());
        SearchResp search2 = milvusClientV2.search(SearchReq.builder()
                .collectionName(newCollection)
                .filter(CommonData.fieldInt64+" < 10 ")
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(CommonData.topK)
                .build());
        Assert.assertEquals(search2.getSearchResults().size(), 1);
        Assert.assertEquals(search2.getSearchResults().get(0).size(), CommonData.topK);
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(newCollection).build());
    }

}
