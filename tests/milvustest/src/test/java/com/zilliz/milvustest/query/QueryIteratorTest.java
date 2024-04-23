package com.zilliz.milvustest.query;

import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.MutationResult;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.param.dml.SearchIteratorParam;
import io.milvus.response.QueryResultsWrapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/4/18 10:01
 */
public class QueryIteratorTest extends BaseTest {
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
        CommonFunction.createIndexWithLoad(collectionWithFloat16Vector,IndexType.HNSW,MetricType.L2,CommonData.defaultFloat16VectorField);

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

    @DataProvider(name = "testData") // limit ,batchSize, expect1, expect2
    public Object[][] provideTestDataParam(){
        return new Object[][]{
                {50L,10L,10,10,collectionWithFloatVector},
                {10L,10L,10,0,collectionWithFloatVector},
                {50L,100L,50,0,collectionWithFloatVector},
                {50L,10L,10,10,collectionWithBinaryVector},
                {10L,10L,10,0,collectionWithBinaryVector},
                {50L,100L,50,0,collectionWithBinaryVector},
                {50L,10L,10,10,collectionWithSparseVector},
                {10L,10L,10,0,collectionWithSparseVector},
                {50L,100L,50,0,collectionWithSparseVector},
                {50L,10L,10,10,collectionWithFloat16Vector},
                {10L,10L,10,0,collectionWithFloat16Vector},
                {50L,100L,50,0,collectionWithFloat16Vector},
                {50L,10L,10,10,collectionWithBF16Vector},
                {10L,10L,10,0,collectionWithBF16Vector},
                {50L,100L,50,0,collectionWithBF16Vector},
        };
    }

    @Test(description = "Query Iterator by float vector collection",groups = {"Smoke"},dataProvider = "testData")
    public void queryIterator(long limit,long batchSize,int expect1,int expect2,String collection) {
        R<QueryIterator> queryIteratorR =
                milvusClient.queryIterator(QueryIteratorParam.newBuilder()
                        .withCollectionName(collection)
                        .withOutFields(Lists.newArrayList("*"))
                        .withBatchSize(batchSize)
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .withLimit(limit)
                        .build());

        Assert.assertEquals(queryIteratorR.getStatus(),0);
        Assert.assertEquals(queryIteratorR.getData().next().size(),expect1);
        Assert.assertEquals(queryIteratorR.getData().next().size(),expect2);
    }



}
