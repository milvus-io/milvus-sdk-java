package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class HybridSearchTest extends BaseTest {

    @DataProvider(name = "multiVector")
    public Object[][] providerData() {
        return new Object[][]{
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "1").dataType(DataType.FloatVector).dim(128).build());
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "2").dataType(DataType.FloatVector).dim(256).build());
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "3").dataType(DataType.FloatVector).dim(768).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "2").dataType(DataType.FloatVector).dim(256).build());
                    add(FieldParam.builder().fieldName(DataType.Float16Vector + "3").dataType(DataType.Float16Vector).dim(768).build());
                    add(FieldParam.builder().fieldName(DataType.BFloat16Vector + "4").dataType(DataType.BFloat16Vector).dim(128).build());
                    add(FieldParam.builder().fieldName(DataType.BinaryVector + "5").dataType(DataType.BinaryVector).dim(768).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.SparseFloatVector + "1").dataType(DataType.SparseFloatVector).dim(CommonData.dim).build());
                    add(FieldParam.builder().fieldName(DataType.Float16Vector + "3").dataType(DataType.Float16Vector).dim(768).build());
                    add(FieldParam.builder().fieldName(DataType.BFloat16Vector + "4").dataType(DataType.BFloat16Vector).dim(128).build());
                    add(FieldParam.builder().fieldName(DataType.BinaryVector + "5").dataType(DataType.BinaryVector).dim(768).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.SparseFloatVector + "1").dataType(DataType.SparseFloatVector).dim(CommonData.dim).build());
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "2").dataType(DataType.FloatVector).dim(256).build());
                    add(FieldParam.builder().fieldName(DataType.BFloat16Vector + "4").dataType(DataType.BFloat16Vector).dim(128).build());
                    add(FieldParam.builder().fieldName(DataType.BinaryVector + "5").dataType(DataType.BinaryVector).dim(768).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.SparseFloatVector + "1").dataType(DataType.SparseFloatVector).dim(CommonData.dim).build());
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "2").dataType(DataType.FloatVector).dim(256).build());
                    add(FieldParam.builder().fieldName(DataType.Float16Vector + "3").dataType(DataType.Float16Vector).dim(768).build());
                    add(FieldParam.builder().fieldName(DataType.BinaryVector + "5").dataType(DataType.BinaryVector).dim(768).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(DataType.SparseFloatVector + "1").dataType(DataType.SparseFloatVector).dim(CommonData.dim).build());
                    add(FieldParam.builder().fieldName(DataType.FloatVector + "2").dataType(DataType.FloatVector).dim(256).build());
                    add(FieldParam.builder().fieldName(DataType.Float16Vector + "3").dataType(DataType.Float16Vector).dim(768).build());
                    add(FieldParam.builder().fieldName(DataType.BFloat16Vector + "4").dataType(DataType.BFloat16Vector).dim(128).build());
                }}
                },


        };
    }


    @Test(description = "Create Multi vector collection", dataProvider = "multiVector")
    public void hybridSearchTest(List<FieldParam> fieldParamList) {
        String s = CommonFunction.genCommonCollection(null, DataType.Int64, false, fieldParamList);
        System.out.println(s);
        // insert
        List<JsonObject> jsonObjects = CommonFunction.genCommonData(s, CommonData.numberEntities);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(s)
                .data(jsonObjects).build());
        // create index
        CommonFunction.createCommonIndex(s, fieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(s).build());
        // load
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(s).build());

        // hybridSearch
        List<AnnSearchReq> AnnSearchReqList = new ArrayList<>();
        for (FieldParam fieldParam : fieldParamList) {
            AnnSearchReq annSearchReq = CommonFunction.provideAnnSearch(fieldParam, 1, 20, null);
            AnnSearchReqList.add(annSearchReq);
        }

        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(s)
                .topK(20)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .searchRequests(AnnSearchReqList)
                .outFields(Lists.newArrayList("*"))
                .ranker(new RRFRanker(60))
                .roundDecimal(-1)
                .build());
        Assert.assertEquals(searchResp.getSearchResults().size(), 1);
        Assert.assertEquals(searchResp.getSearchResults().get(0).size(), 20);
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(s).build());
    }
}
