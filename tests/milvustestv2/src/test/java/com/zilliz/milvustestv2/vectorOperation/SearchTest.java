package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:03
 */
public class SearchTest extends BaseTest {
    int topK = 10;

    @DataProvider(name = "filterAndExcept")
    public Object[][] providerData() {
        return new Object[][]{
                //{CommonData.fieldVarchar + " like \"%0\" ", topK},
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

    @Test(description = "search", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void search(String filter, int expect) {
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .vectorFieldName(CommonData.fieldFloatVector)
                .data(GenerateUtil.generateFloatVector(CommonData.nq, 3, CommonData.dim))
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search in partition", groups = {"Smoke"}, dataProvider = "searchPartition")
    public void searchInPartition(List<String> partitionName, String filter, int expect) {
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .vectorFieldName(CommonData.fieldFloatVector)
                .partitionNames(partitionName)
                .data(GenerateUtil.generateFloatVector(CommonData.nq, 3, CommonData.dim))
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search by alias", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchByAlias(String filter, int expect) {
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.alias)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .vectorFieldName(CommonData.fieldFloatVector)
                .data(GenerateUtil.generateFloatVector(CommonData.nq, 3, CommonData.dim))
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

}
