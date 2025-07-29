package com.zilliz.milvustestv2.alias;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/23 18:14
 */
public class CreateAliasTest extends BaseTest {
    String newCollectionName;
    String newCollectionNameWithDB;
    String aliasName;
    String aliasNameWithDB;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        aliasName = "a_" + GenerateUtil.getRandomString(10);
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.HNSW))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        aliasNameWithDB = "db_" + GenerateUtil.getRandomString(10);
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
        newCollectionNameWithDB = CommonFunction.createNewCollectionWithDatabase(CommonData.dim,null,DataType.FloatVector,CommonData.databaseName2);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropAlias(DropAliasReq.builder().databaseName(CommonData.databaseName2).alias(aliasNameWithDB).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionNameWithDB).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
    }

    @Test(description = "create alias", groups = {"Smoke"})
    public void createAliasTest() {
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionName)
                .alias(aliasName)
                .build());
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().collectionName(newCollectionName).build());
        Assert.assertTrue(listAliasResp.getAlias().contains(aliasName));
        System.out.println(listAliasResp);
    }


    @Test(description = "create alias with database", groups = {"Smoke"})
    public void createAliasWithDatabaseTest() {
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionNameWithDB)
                .databaseName(CommonData.databaseName2)
                .alias(aliasNameWithDB)
                .build());
        DescribeAliasResp describeAliasResp = milvusClientV2.describeAlias(DescribeAliasReq.builder().alias(aliasNameWithDB).databaseName(CommonData.databaseName2).build());
        Assert.assertEquals(describeAliasResp.getCollectionName(),newCollectionNameWithDB);
    }


}
