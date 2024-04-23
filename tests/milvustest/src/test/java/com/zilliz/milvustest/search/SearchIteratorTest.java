package com.zilliz.milvustest.search;

import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchIteratorParam;
import io.milvus.param.dml.ranker.RRFRanker;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/4/15 17:49
 */
@Epic("Search")
@Feature("SearchIterator")
public class SearchIteratorTest extends BaseTest {
    private String collectionWithFloatVector;
    private String collectionWithBinaryVector;
    private String collectionWithSparseVector;

    private String collectionWithFloat16Vector;
    private String collectionWithBF16Vector;

    @BeforeClass(alwaysRun = true)
    public void provideTestData() {
        collectionWithFloatVector = CommonFunction.createNewCollection();
        CommonFunction.insertDataIntoCollection(collectionWithFloatVector, null, 10000);
        CommonFunction.createIndexWithLoad(collectionWithFloatVector, IndexType.HNSW, MetricType.L2, CommonData.defaultVectorField);

        collectionWithBinaryVector = CommonFunction.createBinaryCollection();
        CommonFunction.insertDataIntoCollection(collectionWithBinaryVector, CommonFunction.generateBinaryData(10000));
        CommonFunction.createIndexWithLoad(collectionWithBinaryVector, IndexType.BIN_FLAT, MetricType.HAMMING, CommonData.defaultBinaryVectorField);

        collectionWithSparseVector = CommonFunction.createSparseFloatVectorCollection();
        CommonFunction.insertDataIntoCollection(collectionWithSparseVector, CommonFunction.generateDataWithSparseFloatVector(100000));
        CommonFunction.createIndexWithLoad(collectionWithSparseVector, IndexType.SPARSE_INVERTED_INDEX, MetricType.IP, CommonData.defaultSparseVectorField);


        collectionWithFloat16Vector = CommonFunction.createFloat16Collection();
        CommonFunction.insertDataIntoCollection(collectionWithFloat16Vector,CommonFunction.generateDataWithFloat16Vector(10000));
        CommonFunction.createIndexWithLoad(collectionWithFloat16Vector,IndexType.HNSW,MetricType.IP,CommonData.defaultFloat16VectorField);

        collectionWithBF16Vector = CommonFunction.createBf16Collection();
        CommonFunction.insertDataIntoCollection(collectionWithBF16Vector,CommonFunction.generateDataWithBF16Vector(10000));
        CommonFunction.createIndexWithLoad(collectionWithBF16Vector,IndexType.HNSW,MetricType.L2,CommonData.defaultBF16VectorField);

    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithFloatVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithSparseVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithBinaryVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithFloat16Vector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithBF16Vector).build());
    }

    @DataProvider(name = "testData") // nq,topK,batchSize,expected1,expect2
    public Object[][] provideDataWithExpect(){
        return new Object[][]{
                {1,1,10L,1,0},
                {1,50,50L,50,0},
                {1,50,10L,10,10},
                {1,50,10000L,50,0}
        };
    }

    //nq: 1.Not support search iteration over multiple vectors at present
    @Test(description = "Search Iterator by float vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void searchIterator(int nq,int topK,long bachSize,int expected1,int expected2) {
        R<SearchIterator> searchIteratorR = milvusClient.searchIterator(SearchIteratorParam.newBuilder()
                .withCollectionName(collectionWithFloatVector)
                .withMetricType(MetricType.L2)
                .withOutFields(Lists.newArrayList("*"))
                .withTopK(topK)
                .withVectorFieldName(CommonData.defaultVectorField)
                .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .withBatchSize(bachSize)
                .withFloatVectors(Lists.newArrayList(CommonFunction.generateFloatVectors(nq, CommonData.dim)))
                .build());
        Assert.assertEquals(searchIteratorR.getStatus().intValue(), 0);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected1);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected2);
    }

    @Test(description = "Search Iterator by binary vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void searchIteratorByBinaryVector(int nq,int topK,long batchSize,int expected1,int expected2) {
        long start = System.currentTimeMillis();
        R<SearchIterator> searchIteratorR = milvusClient.searchIterator(SearchIteratorParam.newBuilder()
                .withCollectionName(collectionWithBinaryVector)
                .withMetricType(MetricType.JACCARD)
                .withOutFields(Lists.newArrayList("*"))
                .withTopK(topK)
                .withVectorFieldName(CommonData.defaultBinaryVectorField)
                .withParams(CommonFunction.provideExtraParam(IndexType.BIN_FLAT))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withBatchSize(batchSize)
                .withBinaryVectors(Lists.newArrayList(CommonFunction.generateBinaryVectors(nq, CommonData.dim)))
                .build());
        long end = System.currentTimeMillis();
        System.out.println("cost:"+(end-start)/1000.0);
        Assert.assertEquals(searchIteratorR.getStatus().intValue(), 0);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected1);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected2);
    }

    @Test(description = "Search Iterator by Sparse vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void searchIteratorBySparseVector(int nq,int topK,long batchSize,int expected1,int expected2) {
        long start = System.currentTimeMillis();
        R<SearchIterator> searchIteratorR = milvusClient.searchIterator(SearchIteratorParam.newBuilder()
                .withCollectionName(collectionWithSparseVector)
                .withMetricType(MetricType.IP)
                .withOutFields(Lists.newArrayList("*"))
                .withTopK(topK)
                .withVectorFieldName(CommonData.defaultSparseVectorField)
                .withParams(CommonFunction.provideExtraParam(IndexType.SPARSE_INVERTED_INDEX))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withBatchSize(batchSize)
                .withSparseFloatVectors(Lists.newArrayList(CommonFunction.generateSparseVector()))
                .build());
        long end = System.currentTimeMillis();
        System.out.println("cost:"+(end-start)/1000.0);
        Assert.assertEquals(searchIteratorR.getStatus().intValue(), 0);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected1);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected2);
    }


    @Test(description = "Search Iterator by float16 vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void searchIteratorByFloat16Vector(int nq,int topK,long batchSize,int expected1,int expected2) {
        long start = System.currentTimeMillis();
        R<SearchIterator> searchIteratorR = milvusClient.searchIterator(SearchIteratorParam.newBuilder()
                .withCollectionName(collectionWithFloat16Vector)
                .withMetricType(MetricType.IP)
                .withOutFields(Lists.newArrayList("*"))
                .withTopK(topK)
                .withVectorFieldName(CommonData.defaultFloat16VectorField)
                .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withBatchSize(batchSize)
                .withFloat16Vectors(Lists.newArrayList(CommonFunction.generateFloat16Vectors(CommonData.dim,nq)))
                .build());
        long end = System.currentTimeMillis();
        System.out.println("cost:"+(end-start)/1000.0);
        Assert.assertEquals(searchIteratorR.getStatus().intValue(), 0);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected1);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected2);
    }

    @Test(description = "Search Iterator by bf16 vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void searchIteratorByBF16Vector(int nq,int topK,long batchSize,int expected1,int expected2) {
        long start = System.currentTimeMillis();
        R<SearchIterator> searchIteratorR = milvusClient.searchIterator(SearchIteratorParam.newBuilder()
                .withCollectionName(collectionWithBF16Vector)
                .withMetricType(MetricType.L2)
                .withOutFields(Lists.newArrayList("*"))
                .withTopK(topK)
                .withVectorFieldName(CommonData.defaultBF16VectorField)
                .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withBatchSize(batchSize)
                .withBFloat16Vectors(Lists.newArrayList(CommonFunction.generateBF16Vectors(CommonData.dim,nq)))
                .build());
        long end = System.currentTimeMillis();
        System.out.println("cost:"+(end-start)/1000.0);
        Assert.assertEquals(searchIteratorR.getStatus().intValue(), 0);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected1);
        Assert.assertEquals(searchIteratorR.getData().next().size(),expected2);
    }





}
