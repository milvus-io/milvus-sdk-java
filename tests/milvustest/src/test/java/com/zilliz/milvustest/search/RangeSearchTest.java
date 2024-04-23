package com.zilliz.milvustest.search;


import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.dml.SearchParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2023/10/13 18:23
 * @ 🚗✈️⛴🚳
 */
@Epic("Search")
@Feature("RangeSearch")
public class RangeSearchTest extends BaseTest {
    private String collectionIP;
    private String collectionL2;
    private List<List<Float>>  searchVectorsIP;
    private List<List<Float>>  searchVectorsL2;
    private float  distanceMaxIP;
    private float  distanceMinIP;
    private float  distanceMaxL2;
    private float  distanceMinL2;
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        collectionIP = CommonFunction.createNewCollection();
        collectionL2 = CommonFunction.createNewCollection();
        CommonFunction.insertDataIntoCollection(collectionIP,"default",2000);
        CommonFunction.insertDataIntoCollection(collectionL2,"default",2000);
        CommonFunction.createIndexWithLoad(collectionIP, IndexType.HNSW, MetricType.IP,CommonData.defaultVectorField);
        CommonFunction.createIndexWithLoad(collectionL2, IndexType.HNSW, MetricType.L2,CommonData.defaultVectorField);
        // init distanceIP
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        searchVectorsIP = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionIP)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsIP)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        List<Float> scoresList = searchResultsR.getData().getResults().getScoresList();
        distanceMaxIP=Collections.max(scoresList);
        distanceMinIP=Collections.min(scoresList);
        // init distanceL2
        searchVectorsL2 = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParamL2 =
                SearchParam.newBuilder()
                        .withCollectionName(collectionL2).withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsL2)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsRL2 = milvusClient.search(searchParamL2);
        Assert.assertEquals(searchResultsRL2.getStatus().intValue(), 0);
        List<Float> scoresListL2 = searchResultsRL2.getData().getResults().getScoresList();
        distanceMaxL2=Collections.max(scoresListL2);
        distanceMinL2=Collections.min(scoresListL2);
        System.out.println("distanceMaxIP:"+distanceMaxIP+",distanceMinIP:"+distanceMinIP);
        System.out.println("distanceMaxIP:"+distanceMaxL2+",distanceMinIP:"+distanceMinL2);

    }
    @AfterClass(alwaysRun = true)
    public void deleteTestData(){
        CommonFunction.clearCollection(collectionIP,"default");
        CommonFunction.clearCollection(collectionL2,"default");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection with metric type IP range search",groups = {"Smoke"})
    public void rangeSearchIP(){
        float radius=MathUtil.generalRandomLessThanFloat(distanceMinIP);
        System.out.println("radius:"+radius);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionIP)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsIP)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), SEARCH_K);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection ip range search with illegal radius",groups = {"Smoke"})
    public void rangeSearchIPWithIllegalRadius(){
        float radius=MathUtil.generalRandomLargeThanFloat(distanceMaxIP);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+"}";
        System.out.println("distance:"+distanceMaxIP+",radius:"+radius);
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionIP)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsIP)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection IP range search with range filter",groups = {"Smoke"})
    public void rangeSearchIPWithRangeFilter(){
        float radius=MathUtil.generalRandomLessThanFloat(distanceMinIP);
        float rangeFilter=MathUtil.generalRandomLargeThanFloat(distanceMaxIP);
        System.out.println("radius:"+radius);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+",\"range_filter\":"+rangeFilter+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionIP)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsIP)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), SEARCH_K);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection IP range search with illage range filter",groups = {"Smoke"})
    public void rangeSearchIPWithRangeFilterLessThanRadius(){
        float radius=MathUtil.generalRandomLessThanFloat(distanceMinIP);
        float rangeFilter=MathUtil.generalRandomLessThanFloat(radius);
        System.out.println("radius:"+radius);
        System.out.println("rangeFilter:"+rangeFilter);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+",\"range_filter\":"+rangeFilter+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionIP)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsIP)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 1100);
        Assert.assertTrue(searchResultsR.getException().getMessage().contains("range_filter must be greater than radius"));
    }


    // L2
    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection with metric type L2 range search",groups = {"Smoke"})
    public void rangeSearchL2(){
        float radius=MathUtil.generalRandomLargeThanFloat(distanceMaxL2);
        System.out.println("radius:"+radius);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionL2)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsL2)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), SEARCH_K);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection L2 range search with illegal radius",groups = {"Smoke"})
    public void rangeSearchL2WithIllegalRadius(){
        float radius=MathUtil.generalRandomLessThanFloat(distanceMinL2);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+"}";
        System.out.println("distance:"+distanceMinL2+",radius:"+radius);
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionL2)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsL2)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection L2 range search with range filter",groups = {"Smoke"})
    public void rangeSearchL2WithRangeFilter(){
        float radius=MathUtil.generalRandomLargeThanFloat(distanceMinL2);
        float rangeFilter=MathUtil.generalRandomLessThanFloat(distanceMinL2);
        System.out.println("radius:"+radius);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+",\"range_filter\":"+rangeFilter+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionL2)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withFloatVectors(searchVectorsL2)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultData results = searchResultsR.getData().getResults();
        Assert.assertEquals(results.getScoresList().size(), SEARCH_K);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "collection L2 range search with illegal range filter",groups = {"Smoke"})
    public void rangeSearchL2WithRangeFilterLargeThanRadius(){
        float radius=MathUtil.generalRandomLargeThanFloat(distanceMaxL2);
        float rangeFilter=MathUtil.generalRandomLargeThanFloat(radius);
        System.out.println("radius:"+radius);
        System.out.println("rangeFilter:"+rangeFilter);
        Integer SEARCH_K = 10; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10,\"radius\":"+radius+",\"range_filter\":"+rangeFilter+"}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionL2)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(searchVectorsL2)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 1100);
        Assert.assertTrue(searchResultsR.getException().getMessage().contains("range_filter must be less than radius"));
    }


}
