package com.zilliz.milvustest.mmap;

import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.AlterCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.AnnSearchParam;
import io.milvus.param.dml.HybridSearchParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.dml.ranker.RRFRanker;
import io.milvus.param.index.AlterIndexParam;
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

/**
 * @Author yongpeng.li
 * @Date 2024/4/15 11:20
 */
@Epic("MMAP")
@Feature("AlterIndex")
public class AlterIndexTest extends BaseTest {
    private String collectionWithFloatVector;
    private String collectionWithBinaryVector;
    private String collectionWithSparseVector;
    private String collectionWithMultiVector;

    @BeforeClass(alwaysRun = true)
    public void provideTestData() {
        collectionWithFloatVector = CommonFunction.createNewCollection();
        CommonFunction.insertDataIntoCollection(collectionWithFloatVector, null, 10000);
        CommonFunction.createIndexWithoutLoad(collectionWithFloatVector, IndexType.HNSW, MetricType.L2, CommonData.defaultVectorField);

        collectionWithBinaryVector = CommonFunction.createBinaryCollection();
        CommonFunction.insertDataIntoCollection(collectionWithBinaryVector, CommonFunction.generateBinaryData(10000));
        CommonFunction.createIndexWithoutLoad(collectionWithBinaryVector, IndexType.BIN_IVF_FLAT, MetricType.HAMMING, CommonData.defaultBinaryVectorField);

        collectionWithSparseVector = CommonFunction.createSparseFloatVectorCollection();
        CommonFunction.insertDataIntoCollection(collectionWithSparseVector, CommonFunction.generateDataWithSparseFloatVector(10000));
        CommonFunction.createIndexWithoutLoad(collectionWithSparseVector, IndexType.SPARSE_INVERTED_INDEX, MetricType.IP, CommonData.defaultSparseVectorField);

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
    public void cleanTestData() {
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithFloatVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithSparseVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithBinaryVector).build());
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionWithMultiVector).build());
    }

    @Test(description = "Enable mmap index for  float vector collection", groups = {"Smoke"})
    public void enableMMAPIndexForFloatVectorCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithFloatVector)
                .withIndexName("idx_" + CommonData.defaultVectorField)
                .withMMapEnabled(true)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus(), 0);
    }

    @Test(description = "Enable mmap index for  binary vector collection",groups = {"Smoke"})
    public void enableMMAPIndexForBinaryVectorCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithBinaryVector)
                .withIndexName("idx_"+CommonData.defaultBinaryVectorField)
                .withMMapEnabled(true)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }

    @Test(description = "Enable mmap index for  sparse vector collection",enabled = false)
    public void enableMMAPIndexForSparseVectorCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithSparseVector)
                .withIndexName("idx_"+CommonData.defaultSparseVectorField)
                .withMMapEnabled(true)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus(),0);
    }

    @Test(description = "Enable mmap index for  multi vector collection",groups = {"Smoke"})
    public void enableMMAPIndexForMultiVectorCollection() {
        R<RpcStatus> rpcStatusR1 = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithMultiVector)
                .withIndexName("idx_"+CommonData.defaultVectorField)
                .withMMapEnabled(true)
                .build());
        R<RpcStatus> rpcStatusR2 = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithMultiVector)
                .withIndexName("idx_"+CommonData.defaultBinaryVectorField)
                .withMMapEnabled(true)
                .build());
//        R<RpcStatus> rpcStatusR3 = milvusClient.alterIndex(AlterIndexParam.newBuilder()
//                .withCollectionName(collectionWithMultiVector)
//                .withIndexName("idx_"+CommonData.defaultSparseVectorField)
//                .withMMapEnabled(false)
//                .build());
        Assert.assertEquals(rpcStatusR1.getStatus(),0);
        Assert.assertEquals(rpcStatusR2.getStatus(),0);
//        Assert.assertEquals(rpcStatusR3.getStatus(),0);
    }

    @Test(description = "Enable index mmap failed when the collection is loaded",groups = {"Smoke"},dependsOnMethods = {"enableMMAPIndexForFloatVectorCollection"})
    public void createIndexAfterLoadFloatCollection() {
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionWithFloatVector).build());
        R<RpcStatus> rpcStatusR = milvusClient.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(collectionWithFloatVector)
                .withIndexName("idx_" + CommonData.defaultVectorField)
                .withMMapEnabled(true)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus(),104);
    }

    @Test(description = "Search float vector collection after enable mmap index",groups = {"Smoke"},dependsOnMethods = {"createIndexAfterLoadFloatCollection"})
    public void searchFloatVectorCollectionAfterEnableMMapIndex(){
        Integer SEARCH_K = 2; // TopK
        List<String> search_output_fields = Arrays.asList("*");
        List<List<Float>> search_vectors = CommonFunction.generateFloatVectors(1,CommonData.dim);
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionWithFloatVector)
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

    @Test(description = "search binary vector collection after enable mmap index",groups = {"Smoke"},dependsOnMethods = {"enableMMAPIndexForBinaryVectorCollection"})
    public void searchBinaryVectorCollectionAfterEnableMMapIndex(){
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

    @Test(description = "Hybrid search collection after enable mmap index",groups = {"Smoke"},dependsOnMethods = {"enableMMAPIndexForMultiVectorCollection"})
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
