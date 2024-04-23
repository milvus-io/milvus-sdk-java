package com.zilliz.milvustest.mmap;

import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.AlterCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.AnnSearchParam;
import io.milvus.param.dml.HybridSearchParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.dml.ranker.RRFRanker;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

/**
 * @Author yongpeng.li
 * @Date 2024/4/12 15:21
 */
@Epic("MMAP")
@Feature("AlterCollection")
public class AlterCollectionTest extends BaseTest {
    private String collectionMMapScalar;

    private String collectionWithBinaryVector;
    private String collectionWithSparseVector;
    private String collectionWithMultiVector;

    @BeforeClass(alwaysRun = true)
    public void provideTestData(){
        collectionMMapScalar= CommonFunction.createNewCollection();
        CommonFunction.insertDataIntoCollection(collectionMMapScalar,null,10000);
        CommonFunction.createIndexWithoutLoad(collectionMMapScalar,IndexType.HNSW,MetricType.L2, CommonData.defaultVectorField);

        collectionWithBinaryVector=CommonFunction.createBinaryCollection();
        CommonFunction.insertDataIntoCollection(collectionWithBinaryVector, CommonFunction.generateBinaryData(10000));
        CommonFunction.createIndexWithoutLoad(collectionWithBinaryVector,IndexType.BIN_IVF_FLAT,MetricType.HAMMING,CommonData.defaultBinaryVectorField);

        collectionWithSparseVector = CommonFunction.createSparseFloatVectorCollection();
        CommonFunction.insertDataIntoCollection(collectionWithSparseVector,CommonFunction.generateDataWithSparseFloatVector(10000));
        CommonFunction.createIndexWithoutLoad(collectionWithSparseVector,IndexType.SPARSE_INVERTED_INDEX,MetricType.IP,CommonData.defaultSparseVectorField);

        collectionWithMultiVector = CommonFunction.createMultiVectorCollection();
        List<InsertParam.Field> fields = CommonFunction.generateDataWithMultiVector(10000);
        R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(collectionWithMultiVector)
                .withFields(fields)
                .build());
        Assert.assertEquals(insert.getStatus().intValue(), 0);
        CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.HNSW, MetricType.L2, CommonData.defaultVectorField);
        CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.BIN_FLAT, MetricType.HAMMING, CommonData.defaultBinaryVectorField);
        CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.SPARSE_INVERTED_INDEX, MetricType.IP, CommonData.defaultSparseVectorField);
        CommonFunction.createIndexWithoutLoad(collectionWithMultiVector,IndexType.HNSW,MetricType.L2,CommonData.defaultFloat16VectorField);

    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionMMapScalar).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithSparseVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithMultiVector).build());
    }


    @Test(description = "Enable mmap on scalar field",groups = {"Smoke"})
    public void enableMMAPCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.alterCollection(AlterCollectionParam.newBuilder()
                .withCollectionName(collectionMMapScalar)
                .withMMapEnabled(true)
                .withProperty(Constant.MMAP_ENABLED, Boolean.toString(false)).build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }

    @Test(description = "Enable mmap collection with binary vector",groups = {"Smoke"})
    public void enableMMAPCollectionWithBinaryVector() {
        R<RpcStatus> rpcStatusR = milvusClient.alterCollection(AlterCollectionParam.newBuilder()
                .withCollectionName(collectionWithBinaryVector).withMMapEnabled(true).build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }
    @Test(description = "Enable mmap collection with sparse vector",groups = {"Smoke"})
    public void enableMMAPCollectionWithSparseVector() {
        R<RpcStatus> rpcStatusR = milvusClient.alterCollection(AlterCollectionParam.newBuilder()
                .withCollectionName(collectionWithSparseVector).withMMapEnabled(true).build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }

    @Test(description = "Enable mmap collection with multi vector",groups = {"Smoke"})
    public void enableMMAPCollectionWithMultiVector() {
        R<RpcStatus> rpcStatusR = milvusClient.alterCollection(AlterCollectionParam.newBuilder()
                .withCollectionName(collectionWithMultiVector).withMMapEnabled(true).build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }

    @Test(description = "Enable mmap fail when the collection is loaded",groups = {"Smoke"},dependsOnMethods = {"enableMMAPCollection"})
    public void createIndexAfterLoadFloatCollection() {
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionMMapScalar).build());
        R<RpcStatus> rpcStatusR = milvusClient.alterCollection(AlterCollectionParam.newBuilder()
                .withCollectionName(collectionMMapScalar).withMMapEnabled(true).build());
        Assert.assertEquals(rpcStatusR.getStatus(),104);
    }

    @Test(description = "search collection after enable mmap",groups = {"Smoke"},dependsOnMethods = {"createIndexAfterLoadFloatCollection"})
    public void searchCollectionAfterEnableMMap(){
        Integer SEARCH_K = 2; // TopK
        List<String> search_output_fields = Arrays.asList("*");
        List<List<Float>> search_vectors = CommonFunction.generateFloatVectors(1,CommonData.dim);
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionMMapScalar)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withFloatVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData(CommonData.defaultVectorField, 0).size(), 2);
    }

    @Test(description = "search binary vector collection after enable mmap",groups = {"Smoke"},dependsOnMethods = {"enableMMAPCollectionWithBinaryVector"})
    public void searchBinaryCollectionAfterEnableMMap(){
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(collectionWithBinaryVector).build());
        System.out.println(rpcStatusR);
        Integer SEARCH_K = 2; // TopK
        List<String> search_output_fields = Collections.singletonList("*");
        List<ByteBuffer> byteBuffers = CommonFunction.generateBinaryVectors(1, CommonData.dim);
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionWithBinaryVector)
                        .withMetricType(MetricType.HAMMING)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withBinaryVectors(Lists.newArrayList(byteBuffers))
                        .withVectorFieldName(CommonData.defaultBinaryVectorField)
                        .withParams(CommonFunction.provideExtraParam(IndexType.BIN_IVF_FLAT))
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData(CommonData.defaultBinaryVectorField, 0).size(), 2);
    }

    @Test(description = "search sparse vector collection after enable mmap",groups = {"Smoke"},dependsOnMethods = {"enableMMAPCollectionWithSparseVector"})
    public void searchSparseCollectionAfterEnableMMap(){
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(collectionWithSparseVector).build());
        System.out.println(rpcStatusR);
        Integer SEARCH_K = 2; // TopK
        List<String> search_output_fields = Collections.singletonList("*");
        SortedMap<Long, Float> longFloatSortedMap = CommonFunction.generateSparseVector();
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionWithSparseVector)
                        .withMetricType(MetricType.IP)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withSparseFloatVectors(Lists.newArrayList(longFloatSortedMap))
                        .withVectorFieldName(CommonData.defaultSparseVectorField)
                        .withParams(CommonFunction.provideExtraParam(IndexType.SPARSE_INVERTED_INDEX))
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData(CommonData.defaultSparseVectorField, 0).size(), 2);
    }

    @Test(description = "Hybrid search collection after enable mmap",groups = {"Smoke"},dependsOnMethods = {"enableMMAPCollectionWithMultiVector"})
    public void hybridSearchCollectionAfterEnableMMap(){
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionWithMultiVector)
                .withSyncLoad(true).build());
        // search
        AnnSearchParam floatVSP= AnnSearchParam.newBuilder()
                .withFloatVectors(CommonFunction.generateFloatVectors(1,CommonData.dim))
                .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withVectorFieldName(CommonData.defaultVectorField)
                .withMetricType(MetricType.L2)
                .withTopK(10).build();
        AnnSearchParam binaryVSP= AnnSearchParam.newBuilder()
                .withBinaryVectors(CommonFunction.generateBinaryVectors(1,CommonData.dim))
                .withParams(CommonFunction.provideExtraParam(IndexType.BIN_FLAT))
                .withVectorFieldName(CommonData.defaultBinaryVectorField)
                .withMetricType(MetricType.HAMMING)
                .withTopK(20).build();
        AnnSearchParam sparseVSP= AnnSearchParam.newBuilder()
                .withSparseFloatVectors(Lists.newArrayList(CommonFunction.generateSparseVector()))
                .withParams(CommonFunction.provideExtraParam(IndexType.SPARSE_INVERTED_INDEX))
                .withVectorFieldName(CommonData.defaultSparseVectorField)
                .withMetricType(MetricType.IP)
                .withTopK(30).build();
        AnnSearchParam float16VSP= AnnSearchParam.newBuilder()
                .withFloat16Vectors(CommonFunction.generateFloat16Vectors(CommonData.dim,1))
                .withParams(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withVectorFieldName(CommonData.defaultFloat16VectorField)
                .withMetricType(MetricType.L2)
                .withTopK(30).build();

        HybridSearchParam hybridSearchParam= HybridSearchParam.newBuilder()
                .withCollectionName(collectionWithMultiVector)
                .withOutFields(Lists.newArrayList(CommonData.defaultVectorField,CommonData.defaultBinaryVectorField,CommonData.defaultSparseVectorField))
                .withTopK(40)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .addSearchRequest(binaryVSP)
                .addSearchRequest(sparseVSP)
                .addSearchRequest(floatVSP)
                .addSearchRequest(float16VSP)
                .withRanker(RRFRanker.newBuilder()
                        .withK(2)
                        .build())
                .build();
        R<SearchResults> searchResultsR = milvusClient.hybridSearch(hybridSearchParam);

        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData(CommonData.defaultVectorField, 0).size(), 40);

    }



}
