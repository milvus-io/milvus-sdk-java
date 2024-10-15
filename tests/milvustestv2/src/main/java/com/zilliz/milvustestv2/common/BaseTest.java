package com.zilliz.milvustestv2.common;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.Milvustestv2Application;
import com.zilliz.milvustestv2.config.ConnectInfoConfig;
import com.zilliz.milvustestv2.params.FieldParam;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.param.MetricType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/1/31 14:48
 */
@Slf4j
@SpringBootTest(classes = Milvustestv2Application.class)

public class BaseTest extends AbstractTestNGSpringContextTests {
    public static MilvusClientV2 milvusClientV2 ;

    @Parameters()
    @BeforeSuite(alwaysRun = true)
    public void initCollection() {
        milvusClientV2 = new MilvusClientV2(ConnectConfig.builder()
                .uri( System.getProperty("uri")== null? PropertyFilesUtil.getRunValue("uri"):System.getProperty("uri"))
                .token("root:Milvus")
                .secure(false)
                .connectTimeoutMs(5000L)
                .build());
        logger.info("**************************************************BeforeSuit**********************");
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(CommonData.alias).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultHasNullCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultBFloat16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultFloat16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultBinaryVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultSparseFloatVectorCollection).build());
        initFloatVectorCollectionForTest();
        initHasNoneCollectionForTest();
        initBF16VectorForTest();
        initFloat16VectorForTest();
        initSparseVectorForTest();
        initBinaryVectorForTest();
    }
    @AfterSuite(alwaysRun = true)
    public void cleanTestData() {
        logger.info("**************************************************AfterSuit**********************");
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultBFloat16VectorCollection).build());
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultFloat16VectorCollection).build());
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultBinaryVectorCollection).build());
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(CommonData.defaultSparseFloatVectorCollection).build());
    }

    public  void initFloatVectorCollectionForTest(){
        CommonFunction.createNewCollection(CommonData.dim,CommonData.defaultFloatVectorCollection, DataType.FloatVector);
        milvusClientV2.createAlias(CreateAliasReq.builder().collectionName(CommonData.defaultFloatVectorCollection).alias(CommonData.alias).build());
        // insert data
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultFloatVectorCollection).data(jsonObjects).build());
        CommonFunction.createVectorIndex(CommonData.defaultFloatVectorCollection,CommonData.fieldFloatVector, IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
        log.info("insert default float vector collection:"+insert);
        // create partition
       CommonFunction.createPartition(CommonData.defaultFloatVectorCollection,CommonData.partitionNameA);
       CommonFunction.createPartition(CommonData.defaultFloatVectorCollection,CommonData.partitionNameB);
       CommonFunction.createPartition(CommonData.defaultFloatVectorCollection,CommonData.partitionNameC);
        List<JsonObject> jsonObjectsA = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        List<JsonObject> jsonObjectsB = CommonFunction.generateDefaultData(0,CommonData.numberEntities*2, CommonData.dim,DataType.FloatVector);
        List<JsonObject> jsonObjectsC = CommonFunction.generateDefaultData(0,CommonData.numberEntities*3, CommonData.dim,DataType.FloatVector);
       milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultFloatVectorCollection).partitionName(CommonData.partitionNameA).data(jsonObjectsA).build());
       milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultFloatVectorCollection).partitionName(CommonData.partitionNameB).data(jsonObjectsB).build());
       milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultFloatVectorCollection).partitionName(CommonData.partitionNameC).data(jsonObjectsC).build());
    }

    public void initBF16VectorForTest(){
        CommonFunction.createNewCollection(CommonData.dim,CommonData.defaultBFloat16VectorCollection, DataType.BFloat16Vector);
        CommonFunction.createIndexAndInsertAndLoad(CommonData.defaultBFloat16VectorCollection,DataType.BFloat16Vector,true,CommonData.numberEntities);
    }
    public void initFloat16VectorForTest(){
        CommonFunction.createNewCollection(CommonData.dim,CommonData.defaultFloat16VectorCollection, DataType.Float16Vector);
        CommonFunction.createIndexAndInsertAndLoad(CommonData.defaultFloat16VectorCollection,DataType.Float16Vector,true,CommonData.numberEntities);
    }
    public void initBinaryVectorForTest(){
        CommonFunction.createNewCollection(CommonData.dim,CommonData.defaultBinaryVectorCollection, DataType.BinaryVector);
        CommonFunction.createIndexAndInsertAndLoad(CommonData.defaultBinaryVectorCollection,DataType.BinaryVector,true,CommonData.numberEntities);
    }

    public void initSparseVectorForTest(){
        CommonFunction.createNewCollection(CommonData.dim,CommonData.defaultSparseFloatVectorCollection, DataType.SparseFloatVector);
        CommonFunction.createIndexAndInsertAndLoad(CommonData.defaultSparseFloatVectorCollection,DataType.SparseFloatVector,true,CommonData.numberEntities);
    }

    public void initHasNoneCollectionForTest(){
        CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, CommonData.defaultHasNullCollection, DataType.FloatVector);
        // insert data
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultHasNullCollection).data(jsonObjects).build());
        CommonFunction.createVectorIndex(CommonData.defaultHasNullCollection,CommonData.fieldFloatVector, IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.L2);
        // Build Scalar Index
        List<FieldParam> FieldParamList = new ArrayList<FieldParam>() {{
            add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt8).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt32).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt64).indextype(IndexParam.IndexType.STL_SORT).build());
            add(FieldParam.builder().fieldName(CommonData.fieldBool).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldArray).indextype(IndexParam.IndexType.BITMAP).build());
        }};
        CommonFunction.createScalarCommonIndex(CommonData.defaultHasNullCollection, FieldParamList);
        log.info("Create Scalar index done{}, scalar index:{}", CommonData.defaultHasNullCollection, FieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(CommonData.defaultHasNullCollection).build());
        log.info("insert default float vector collection:" + insert);
        // create partition
        CommonFunction.createPartition(CommonData.defaultHasNullCollection,CommonData.partitionNameA);
        CommonFunction.createPartition(CommonData.defaultHasNullCollection,CommonData.partitionNameB);
        CommonFunction.createPartition(CommonData.defaultHasNullCollection,CommonData.partitionNameC);
        List<JsonObject> jsonObjectsA = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        List<JsonObject> jsonObjectsB = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities*2, CommonData.dim,DataType.FloatVector);
        List<JsonObject> jsonObjectsC = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities*3, CommonData.dim,DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultHasNullCollection).partitionName(CommonData.partitionNameA).data(jsonObjectsA).build());
        milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultHasNullCollection).partitionName(CommonData.partitionNameB).data(jsonObjectsB).build());
        milvusClientV2.insert(InsertReq.builder().collectionName(CommonData.defaultHasNullCollection).partitionName(CommonData.partitionNameC).data(jsonObjectsC).build());
    }
}
