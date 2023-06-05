package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.*;
import io.milvus.response.QueryResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FilterOutputStream;
import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2023/5/29 09:22
 */
@Epic("Partition")
@Feature("PartitionKey")
public class PartitionKeyTest extends BaseTest {
    private String collection;
    private int partitionKeyNum = 20;

    @BeforeClass(description = "init collection with partition Key", alwaysRun = true)
    public void provideCollection() {
        collection = CommonFunction.createNewCollectionWithPartitionKey(partitionKeyNum);

    }

    @DataProvider(name = "provideExpressions")
    public Object[][] provideStringExpression() {
        return new Object[][]{
                {" book_name in [\"part0\"]"},
                {" book_name == \"part0\""},
                {" book_name >= \"part0\""},
        };
    }

    @AfterClass(description = "clean test data ", alwaysRun = true)
    public void cleanTestData() {
        milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collection).build());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "check partition num", groups = {"Smoke"})
    public void checkPartitionInCollection() {
        R<ShowPartitionsResponse> showPartitionsResponseR = milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(collection).build());
        Assert.assertEquals(showPartitionsResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(showPartitionsResponseR.getData().getPartitionIDsCount(), partitionKeyNum);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "Insert data into collection with partition key", groups = {"Smoke"})
    public void insertData() {
        List<InsertParam.Field> fields = CommonFunction.generateDataWithPartitionKey(10000);
        // insert
        R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
                .withFields(fields)
                .withCollectionName(collection)
                .build());
        Assert.assertEquals(insert.getStatus().intValue(), 0);
        Assert.assertEquals(insert.getData().getSuccIndexCount(), 10000);

        // createindex
        R<RpcStatus> index = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.L2)
                .withFieldName(CommonData.defaultVectorField)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withSyncMode(true)
                .withSyncWaitingInterval(50L)
                .withSyncWaitingTimeout(30L)
                .build());
        Assert.assertEquals(index.getStatus().intValue(), 0);
        // load
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collection)
                .withSyncLoad(true)
                .withSyncLoadWaitingInterval(50L)
                .withSyncLoadWaitingTimeout(300L).build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
        System.out.println(rpcStatusR.toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "search", groups = {"Smoke"}, dependsOnMethods = {"insertData"}, dataProvider = "provideExpressions")
    public void search(String expr) {
        Integer SEARCH_K = 2; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList(CommonData.defaultPartitionField);
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collection)
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withExpr(expr)
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
        Assert.assertEquals(searchResultsR.getData().getResults().getTopK(), SEARCH_K.intValue());
        Assert.assertEquals(searchResultsR.getData().getResults().getFieldsDataList().get(0).getFieldName(), CommonData.defaultPartitionField);


    }

    @Severity(SeverityLevel.CRITICAL)
    @Test(description = "search", dependsOnMethods = {"insertData"}, dataProvider = "provideExpressions")
    public void query(String expr) {
        List<String> query_output_fields = Collections.singletonList(CommonData.defaultPartitionField);
        QueryParam queryParam =
                QueryParam.newBuilder()
                        .withCollectionName(collection)
                        .withOutFields(query_output_fields)
                        .withExpr(expr)
                        .build();
        R<QueryResults> queryResultsR = milvusClient.query(queryParam);
        Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
        QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
        Assert.assertEquals(wrapperQuery.getFieldWrapper("book_name").getFieldData().get(0).toString(), "part0");
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Use Partition key field type of int32", expectedExceptions = ParamException.class)
    public void createCollectionWithIllegalPartition() {
        String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
                        .withName(CommonData.defaultVectorField)
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        FieldType fieldType4 = FieldType.newBuilder()
                .withName(CommonData.defaultPartitionField)
                .withDataType(DataType.Int32)
                .withMaxLength(128)
                .withPartitionKey(true)
                .build();
        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test" + collectionName + "search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .addFieldType(fieldType4)
                        .withPartitionsNum(partitionKeyNum)
                        .build();
        R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Set Partition key field primary key ", expectedExceptions = ParamException.class)
    public void createCollectionWithIllegalPK() {
        String collectionName = "Collection_" + MathUtil.getRandomString(10);
        FieldType fieldType1 =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withPartitionKey(true)
                        .withAutoID(false)
                        .build();
        FieldType fieldType2 =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType fieldType3 =
                FieldType.newBuilder()
                        .withName(CommonData.defaultVectorField)
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();

        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test" + collectionName + "search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .withPartitionsNum(partitionKeyNum)
                        .build();
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "partition num over 4096")
    public void createCollectionWith4097Partitions() {
        String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
                        .withName(CommonData.defaultVectorField)
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        FieldType fieldType4 = FieldType.newBuilder()
                .withName(CommonData.defaultPartitionField)
                .withDataType(DataType.Int64)
                .withMaxLength(128)
                .withPartitionKey(true)
                .build();
        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test" + collectionName + "search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .addFieldType(fieldType4)
                        .withPartitionsNum(4097)
                        .build();
        R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
        Assert.assertEquals(collection.getStatus().intValue(), 1);
        Assert.assertTrue(collection.getException().getMessage().contains("partition number (4097) exceeds max configuration (4096)"));
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Create partition use partition name param in  Partition key mode")
    public void createPartitionWithPartitionName() {
        R<RpcStatus> partition = milvusClient.createPartition(CreatePartitionParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionName("part000").build());
        Assert.assertEquals(partition.getStatus().intValue(), 1);
        Assert.assertTrue(partition.getException().getMessage().contains("disable create partition if partition key mode is used"));
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Drop partition use partition name param in  Partition key mode")
    public void dropPartitionWithPartitionName() {
        R<RpcStatus> rpcStatusR = milvusClient.dropPartition(DropPartitionParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionName("_default_1").build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
        Assert.assertTrue(rpcStatusR.getException().getMessage().contains("disable drop partition if partition key mode is used"));
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Load partition use partition name param in  Partition key mode")
    public void loadPartitionWithPartitionName() {
        R<RpcStatus> rpcStatusR = milvusClient.loadPartitions(LoadPartitionsParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionNames(Collections.singletonList(CommonData.defaultPartitionField))
                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
        Assert.assertTrue(rpcStatusR.getException().getMessage().contains("disable load partitions if partition key mode is used"));
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "release partition use partition name param in  Partition key mode")
    public void releasePartitionWithPartitionName() {
        R<RpcStatus> rpcStatusR = milvusClient.releasePartitions(ReleasePartitionsParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionNames(Collections.singletonList("_default_1"))
                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
        Assert.assertTrue(rpcStatusR.getException().getMessage().contains("disable release partitions if partition key mode is used"));
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Show partition use partition name param in  Partition key mode")
    public void showPartitionsWithPartitionName() {
        R<ShowPartitionsResponse> responseR = milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionNames(Collections.singletonList("_default_1"))
                .build());
        Assert.assertEquals(responseR.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Get partition use partition name param in  Partition key mode")
    public void getPartitionsWithPartitionName() {
        R<GetPartitionStatisticsResponse> responseR = milvusClient.getPartitionStatistics(GetPartitionStatisticsParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionName("_default_1")
                .build());
        Assert.assertEquals(responseR.getStatus().intValue(), 0);
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Has partition use partition name param in  Partition key mode")
    public void hasPartitionsWithPartitionName() {
        R<Boolean> booleanR = milvusClient.hasPartition(HasPartitionParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionName("_default_1").build());
        Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    }


    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Insert use partition name param in  Partition key mode")
    public void InsertWithPartitionName() {
        List<InsertParam.Field> fields = CommonFunction.generateDataWithPartitionKey(10000);
        // insert
        R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
                .withFields(fields)
                .withCollectionName(collection)
                .withPartitionName(CommonData.defaultPartitionField)
                .build());
        Assert.assertEquals(insert.getStatus().intValue(), 1);
    }


    @Severity(SeverityLevel.NORMAL)
    @Test(description = "search use partition name param in  Partition key mode")
    public void searchWithPartitionName() {
        String expr=" book_name in [\"part0\"]";
        Integer SEARCH_K = 2; // TopK
        String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Collections.singletonList(CommonData.defaultPartitionField);
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        SearchParam searchParam =
                SearchParam.newBuilder()
                        .withCollectionName(collection)
                        .withPartitionNames(Collections.singletonList(CommonData.defaultPartitionField))
                        .withMetricType(MetricType.L2)
                        .withOutFields(search_output_fields)
                        .withTopK(SEARCH_K)
                        .withVectors(search_vectors)
                        .withVectorFieldName(CommonData.defaultVectorField)
                        .withParams(SEARCH_PARAM)
                        .withExpr(expr)
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .build();
        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
        Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "query use partition name param in  Partition key mode")
    public void queryWithPartitionName() {
        String expr=" book_name in [\"part0\"]";
        List<String> query_output_fields = Collections.singletonList(CommonData.defaultPartitionField);
        QueryParam queryParam =
                QueryParam.newBuilder()
                        .withCollectionName(collection)
                        .withPartitionNames(Collections.singletonList(CommonData.defaultPartitionField))
                        .withOutFields(query_output_fields)
                        .withExpr(expr)
                        .build();
        R<QueryResults> queryResultsR = milvusClient.query(queryParam);
        Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "delete use partition name param in  Partition key mode")
    public void deleteWithPartitionName() {
        String expr=" book_name in [\"part0\"]";
        R<MutationResult> delete = milvusClient.delete(DeleteParam.newBuilder()
                .withCollectionName(collection)
                .withExpr(expr)
                .withPartitionName(CommonData.defaultPartitionField)
                .build());
        Assert.assertEquals(delete.getStatus().intValue(),1);
    }


}
