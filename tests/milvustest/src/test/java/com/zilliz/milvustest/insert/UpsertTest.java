package com.zilliz.milvustest.insert;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.dml.UpsertParam;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2023/8/22 11:19
 */
@Epic("Insert")
@Feature("UpsertTest")
public class UpsertTest extends BaseTest {
    @BeforeTest(alwaysRun = true)
    public void insertDataIntoCollection() {
        List<InsertParam.Field> fields = CommonFunction.generateData(2000);
        R<MutationResult> mutationResultR =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(CommonData.defaultCollection)
                                .withFields(fields)
                                .build());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "UpsertTest data into collection",groups = {"Smoke"})
    public void upsertDataIntoCollection() {
        Random ran = new Random();
        List<Long> book_id_array = new ArrayList<>();
        List<Long> word_count_array = new ArrayList<>();
        List<List<Float>> book_intro_array = new ArrayList<>();
        for (long i = 0L; i < 10; ++i) {
            book_id_array.add(i);
            word_count_array.add(i + 20000);
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < 128; ++k) {
                vector.add(ran.nextFloat());
            }
            book_intro_array.add(vector);
        }
        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("book_id", book_id_array));
        fields.add(new InsertParam.Field("word_count", word_count_array));
        fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
        R<MutationResult> mutationResultR =
                milvusClient.upsert(UpsertParam.newBuilder()
                        .withCollectionName(CommonData.defaultCollection)
                        .withFields(fields).build());
        Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
        Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 10);
        Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 10);
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "Search after upsert collection",groups = {"Smoke"},dependsOnMethods = {"upsertDataIntoCollection"})
    public void searchAfterUpsertCollection(){
        //load
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withSyncLoad(true).build());
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList("word_count");
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(CommonData.defaultCollection)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withFloatVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .withExpr("book_id < 10")
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).size(), 10);
    }



}
