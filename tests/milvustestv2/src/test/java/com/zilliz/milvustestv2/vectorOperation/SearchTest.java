package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.DataProviderUtils;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:03
 */
public class SearchTest extends BaseTest {
    int topK = 10;
    private MilvusServiceClient milvusServiceClient;

    @DataProvider(name = "filterAndExcept")
    public Object[][] providerData() {
        return new Object[][]{
                {CommonData.fieldInt64 + " != 10 ", topK},
                {CommonData.fieldInt64 + " < 10 ", topK},
                {CommonData.fieldVarchar + " like \"%0\" ", topK},
                {CommonData.fieldInt64 + " <= 10 ", topK},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", topK},
                {CommonData.fieldInt64 + " > 100 ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + "== true", topK / 2},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + ">5", 4},
                {CommonData.fieldVarchar + " > \"Str5\" ", topK},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", topK},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},

        };
    }

    @DataProvider(name = "VectorTypeList")
    public Object[][] providerVectorType() {
        return new Object[][]{
                {CommonData.defaultFloatVectorCollection, DataType.FloatVector},
//                {CommonData.defaultBinaryVectorCollection,DataType.BinaryVector},
                {CommonData.defaultFloat16VectorCollection,DataType.Float16Vector},
                {CommonData.defaultBFloat16VectorCollection,DataType.BFloat16Vector},
                {CommonData.defaultSparseFloatVectorCollection,DataType.SparseFloatVector},
        };
    }

    @DataProvider(name="VectorTypeWithFilter")
    public Object[][] providerVectorTypeWithFilter(){
        Object[][] vectorType=new Object[][]{
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.Float16Vector},
                {DataType.BFloat16Vector},
                {DataType.SparseFloatVector}
        };
        Object[][] filter=new Object[][]{
                {CommonData.fieldVarchar + " like \"%0\" ", topK},
                {CommonData.fieldInt64 + " < 10 ", topK},
                {CommonData.fieldInt64 + " != 10 ", topK},
                {CommonData.fieldInt64 + " <= 10 ", topK},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", topK},
                {CommonData.fieldInt64 + " > 100 ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + "== true", topK / 2},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + ">5", 4},
                {CommonData.fieldVarchar + " > \"Str5\" ", topK},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", topK},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},

        };
        Object[][] objects = DataProviderUtils.generateDataSets(vectorType, filter);
        return objects;
    }

    @DataProvider(name = "searchPartition")
    private Object[][] providePartitionSearchParams() {
        return new Object[][]{
                {Lists.newArrayList(CommonData.partitionNameA), "0 < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities, topK},
                {Lists.newArrayList(CommonData.partitionNameA), CommonData.numberEntities + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 2, 0},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 2, topK},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities * 2 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 3, 0},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 2 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 3, topK},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 3 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 4, 0}
        };
    }

    @Test(description = "search float vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchFloatVectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search binary vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchBinaryVectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.BinaryVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultBinaryVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search bf16 vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchBF16VectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.BFloat16Vector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultBFloat16VectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search float16 vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchFloat16VectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.Float16Vector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloat16VectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search Sparse vector collection", groups = {"Smoke"})
    public void searchSparseVectorCollection() {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.SparseFloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultSparseFloatVectorCollection)
                .filter("fieldVarchar like\"%0\"")
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
//        Assert.assertEquals(search.getSearchResults().get(0).size(), topK);
    }

    @Test(description = "default search output params return id and distance", groups = {"Smoke"})
    public void searchWithDefaultOutput() {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(CommonData.fieldInt64 + " < 10 ")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), topK);
        Assert.assertEquals(search.getSearchResults().get(0).get(0).getEntity().keySet().size(), 0);
    }

    @Test(description = "search in partition", groups = {"Smoke"}, dataProvider = "searchPartition")
    public void searchInPartition(List<String> partitionName, String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .partitionNames(partitionName)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search by alias", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchByAlias(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.alias)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search group by field name", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchByGroupByField(String collectionName,DataType vectorType){
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                        .groupByFieldName(CommonData.fieldInt8)
                .data(data)
                .topK(1000)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if(vectorType!=DataType.SparseFloatVector) {
            Assert.assertEquals(search.getSearchResults().get(0).size(), 127);
        }
    }
}
