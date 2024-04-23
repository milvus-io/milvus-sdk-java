package com.zilliz.milvustest.database;

import com.fasterxml.jackson.databind.ser.Serializers;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2023/10/11 15:27
 */
@Epic("Database")
@Feature("Database")
public class DatabaseTest extends BaseTest {
    public String collectionWithDatabase;
    public MilvusServiceClient milvusClient1;
    public MilvusServiceClient milvusClient2;

    @BeforeClass(description = "init testdata", alwaysRun = true)
    public void initTestData(){
        collectionWithDatabase="collection_" + MathUtil.getRandomString(10);
        milvusClient1 =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(
                                        System.getProperty("milvusHost") == null
                                                ? PropertyFilesUtil.getRunValue("milvusHost")
                                                : System.getProperty("milvusHost"))
                                .withPort(
                                        Integer.parseInt(
                                                System.getProperty("milvusPort") == null
                                                        ? PropertyFilesUtil.getRunValue("milvusPort")
                                                        : System.getProperty("milvusPort")))
                                .withDatabaseName(CommonData.databaseName1)
                                //.withSecure(true)
                                .build());
        milvusClient2 =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(
                                        System.getProperty("milvusHost") == null
                                                ? PropertyFilesUtil.getRunValue("milvusHost")
                                                : System.getProperty("milvusHost"))
                                .withPort(
                                        Integer.parseInt(
                                                System.getProperty("milvusPort") == null
                                                        ? PropertyFilesUtil.getRunValue("milvusPort")
                                                        : System.getProperty("milvusPort")))
                                .withDatabaseName(CommonData.databaseName2)
                                //.withSecure(true)
                                .build());
    }

    @AfterClass(description = "delete test datas after CreateCollectionTest", alwaysRun = true)
    public void deleteTestData() {
        if (collectionWithDatabase != null) {
            milvusClient.dropCollection(
                    DropCollectionParam.newBuilder().withCollectionName(collectionWithDatabase)
                            .withDatabaseName(CommonData.databaseName1).build());
            milvusClient.dropCollection(
                    DropCollectionParam.newBuilder().withCollectionName(collectionWithDatabase)
                            .withDatabaseName(CommonData.databaseName2).build());
        }
        milvusClient.dropDatabase(DropDatabaseParam.newBuilder().withDatabaseName(CommonData.databaseName2).build());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "create database",groups = {"Smoke"})
    public void createDatabase(){
        R<RpcStatus> database = milvusClient.createDatabase(CreateDatabaseParam.newBuilder().withDatabaseName(CommonData.databaseName2).build());
        Assert.assertEquals(database.getStatus().toString(), "0");
        Assert.assertEquals(database.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.CRITICAL)
    @Test(description = "create database with empty name",expectedExceptions = ParamException.class)
    public void createDatabaseWithEmptyName(){
        milvusClient.createDatabase(CreateDatabaseParam.newBuilder().build());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "list database",groups = {"Smoke"})
    public void listDatabase(){
        R<ListDatabasesResponse> listDatabasesResponseR = milvusClient.listDatabases();
        Assert.assertEquals(listDatabasesResponseR.getStatus().toString(), "0");
        Assert.assertTrue(listDatabasesResponseR.getData().getDbNamesList().size()>=2);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "Create collection with database success",
            groups = {"Smoke"})
    public void createCollectionWithDatabase() {
        FieldType fieldType1 =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType fieldType2 =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType fieldType3 =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withDescription("Test " + collectionWithDatabase + " search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .withDatabaseName(CommonData.databaseName1)
                        .build();
        R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
        Assert.assertEquals(collection.getStatus().toString(), "0");
        Assert.assertEquals(collection.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "Create same collection name with diff database",
            dependsOnMethods = {"createCollectionWithDatabase","createDatabase"},
            groups = {"Smoke"})
    public void createSameCollectionNameInDiffDatabase() {
        FieldType fieldType1 =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType fieldType2 =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType fieldType3 =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withDescription("Test " + collectionWithDatabase + " search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .withDatabaseName(CommonData.databaseName2)
                        .build();
        R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
        Assert.assertEquals(collection.getStatus().toString(), "0");
        Assert.assertEquals(collection.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "drop default database",
            groups = {"Smoke"})
    public void dropDefaultDatabase(){
        R<RpcStatus> aDefault = milvusClient.dropDatabase(DropDatabaseParam.newBuilder().withDatabaseName("default").build());
        Assert.assertEquals(aDefault.getStatus().intValue(), 65535);
        Assert.assertTrue(aDefault.getException().getMessage().contains("can not drop default"));
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "insert with database",
            groups = {"Smoke"})
    public void insertWithDatabase(){
        List<InsertParam.Field> fields = CommonFunction.generateData(2000);
        R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(collectionWithDatabase)
                .withDatabaseName(CommonData.databaseName1)
                .withFields(fields).build());
        Assert.assertEquals(insert.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "create index with database",
            groups = {"Smoke"})
    public void createIndexWithDatabase(){
        R<RpcStatus> index = milvusClient.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withFieldName(CommonData.defaultVectorField)
                        .withIndexName(CommonData.defaultIndex)
                        .withMetricType(MetricType.L2)
                        .withIndexType(IndexType.HNSW)
                        .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                        .withSyncMode(Boolean.FALSE)
                        .withDatabaseName(CommonData.databaseName1)
                        .build());
        Assert.assertEquals(index.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "flush with database",
            groups = {"Smoke"})
    public void flushWithDatabase(){
        R<FlushResponse> flush = milvusClient.flush(FlushParam.newBuilder()
                .withCollectionNames(Collections.singletonList(collectionWithDatabase))
                .withSyncFlush(true)
                .withDatabaseName(CommonData.databaseName1)
                .build());
        Assert.assertEquals(flush.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "load with database",
            groups = {"Smoke"})
    public void loadWithDatabase(){
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionWithDatabase)
                .withDatabaseName(CommonData.databaseName1)
                .withSyncLoad(true)
                .withSyncLoadWaitingTimeout(30L)
                .withSyncLoadWaitingInterval(50L)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
    }




    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "search with database",
            groups = {"Smoke"},
    dependsOnMethods = {"createCollectionWithDatabase"})
    public void searchWithDatabaseConnectAssignDB(){
        // prepare collection
        CommonFunction.prepareCollectionForSearch(collectionWithDatabase,CommonData.databaseName1);
        // search
        Integer SEARCH_K = 2; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient1.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
        System.out.println(searchResultsR.getData().getResults());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "search nonexistent collection with database",
            groups = {"Smoke"},
            dependsOnMethods = {"createCollectionWithDatabase"})
    public void searchNonexistentCollectionWithDatabase(){
        // search
        Integer SEARCH_K = 2; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(CommonData.defaultCollection)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient1.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 100);
        Assert.assertTrue(searchResultsR.getException().getMessage().contains("collection not found"));
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "query with database",
            groups = {"Smoke"},
            dependsOnMethods = {"createCollectionWithDatabase"})
    public void queryWithDatabase(){
        String SEARCH_PARAM = "book_id in [2,4,6,8]";
        List<String> outFields = Arrays.asList("book_id", "word_count", CommonData.defaultVectorField);
        QueryParam queryParam =
                QueryParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withOutFields(outFields)
                        .withExpr(SEARCH_PARAM)
                        .build();
        R<QueryResults> queryResultsR = milvusClient1.query(queryParam);
        QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
        Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
        Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
        Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
        Assert.assertEquals(wrapperQuery.getFieldWrapper(CommonData.defaultVectorField).getDim(), 128);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "query with database",
            groups = {"Smoke"},
            dependsOnMethods = {"createCollectionWithDatabase"})
    public void queryNonexistentCollectionWithDatabase(){
        String SEARCH_PARAM = "book_id in [2,4,6,8]";
        List<String> outFields = Arrays.asList("book_id", "word_count", CommonData.defaultVectorField);
        QueryParam queryParam =
                QueryParam.newBuilder()
                        .withCollectionName(CommonData.defaultCollection)
                        .withOutFields(outFields)
                        .withExpr(SEARCH_PARAM)
                        .build();
        R<QueryResults> queryResultsR = milvusClient1.query(queryParam);
        Assert.assertEquals(queryResultsR.getStatus().intValue(), 100);
        Assert.assertTrue(queryResultsR.getException().getMessage().contains("collection not found"));
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "insert with database",dependsOnMethods = {"createSameCollectionNameInDiffDatabase"},
            groups = {"Smoke"})
    public void insertWithAntherDatabase(){
        List<InsertParam.Field> fields = CommonFunction.generateData(2000);
        R<MutationResult> insert = milvusClient1.insert(InsertParam.newBuilder()
                .withCollectionName(collectionWithDatabase)
                .withDatabaseName(CommonData.databaseName2)
                .withFields(fields).build());
        Assert.assertEquals(insert.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "create index with database",dependsOnMethods = {"insertWithAntherDatabase"},
            groups = {"Smoke"})
    public void createIndexWithAntherDatabase(){
        R<RpcStatus> index = milvusClient1.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withFieldName(CommonData.defaultVectorField)
                        .withIndexName(CommonData.defaultIndex)
                        .withMetricType(MetricType.L2)
                        .withIndexType(IndexType.HNSW)
                        .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                        .withSyncMode(Boolean.FALSE)
                        .withDatabaseName(CommonData.databaseName2)
                        .build());
        Assert.assertEquals(index.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "flush with database",dependsOnMethods = {"createIndexWithAntherDatabase"},
            groups = {"Smoke"})
    public void flushWithAntherDatabase(){
        R<FlushResponse> flush = milvusClient1.flush(FlushParam.newBuilder()
                .withCollectionNames(Collections.singletonList(collectionWithDatabase))
                .withSyncFlush(true)
                .withDatabaseName(CommonData.databaseName2)
                .build());
        Assert.assertEquals(flush.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "load with database",dependsOnMethods = {"flushWithAntherDatabase"},
            groups = {"Smoke"})
    public void loadWithAntherDatabase(){
        R<RpcStatus> rpcStatusR = milvusClient1.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionWithDatabase)
                .withDatabaseName(CommonData.databaseName2)
                .withSyncLoad(true)
                .withSyncLoadWaitingTimeout(30L)
                .withSyncLoadWaitingInterval(50L)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
    }
    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "search with database",
            groups = {"Smoke"},
            dependsOnMethods = {"loadWithAntherDatabase"})
    public void searchWithDatabaseConnectAssignDB2(){
        // prepare collection
        CommonFunction.prepareCollectionForSearch(collectionWithDatabase,CommonData.databaseName1);
        // search
        Integer SEARCH_K = 2; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList("book_id");
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collectionWithDatabase)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .build();
        R<SearchResults> searchResultsR = milvusClient2.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        SearchResultsWrapper searchResultsWrapper =
                new SearchResultsWrapper(searchResultsR.getData().getResults());
        Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
        System.out.println(searchResultsR.getData().getResults());

    }
}
