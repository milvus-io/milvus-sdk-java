package com.zilliz.milvustest.vdc;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.*;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.*;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @Author yongpeng.li @Date 2022/9/22 14:54
 */
@Epic("Role")
@Feature("VDC")
public class DBAdminRBACTest extends BaseTest {
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "db_admin privilege verify")
  public void dbAdminPrivilegeVerify() {
    // privilege:CreateCollection
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
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
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().intValue(), 0);
    // DescribeCollection
    R<DescribeCollectionResponse> describeCollectionResponseR =
        milvusClient.describeCollection(
            DescribeCollectionParam.newBuilder().withCollectionName(collectionName).build());
    Assert.assertEquals(describeCollectionResponseR.getStatus().intValue(), 0);
    // ShowCollections
    R<ShowCollectionsResponse> showCollectionsResponseR =
        milvusClient.showCollections(ShowCollectionsParam.newBuilder().build());
    Assert.assertEquals(showCollectionsResponseR.getStatus().intValue(), 0);
    // insert
    List<InsertParam.Field> fields = CommonFunction.generateData(10000);
    R<MutationResult> insert =
            milvusClient.insert(
                    InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    // CreateIndex
    R<RpcStatus> index =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(MetricType.L2)
                .withIndexType(IndexType.AUTOINDEX)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    Assert.assertEquals(index.getStatus().intValue(), 0);
    // IndexDetail--DescribeIndex,GetIndexBuildProgress,GetIndexState
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 0);
    R<GetIndexBuildProgressResponse> indexBuildProgress =
        milvusClient.getIndexBuildProgress(
            GetIndexBuildProgressParam.newBuilder()
                .withIndexName(CommonData.defaultIndex)
                .withCollectionName(collectionName)
                .build());
    Assert.assertEquals(indexBuildProgress.getStatus().intValue(), 0);
    R<GetIndexStateResponse> indexState =
        milvusClient.getIndexState(
            GetIndexStateParam.newBuilder()
                .withIndexName(CommonData.defaultIndex)
                .withCollectionName(collectionName)
                .build());
    Assert.assertEquals(indexState.getStatus().intValue(), 0);
    // load
    R<RpcStatus> rpcStatusR3 =
        milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(true)
                .withSyncLoadWaitingInterval(500L)
                .withSyncLoadWaitingTimeout(30L)
                .build());
    Assert.assertEquals(rpcStatusR3.getStatus().intValue(), 0);


    // search
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(collectionName)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    R<SearchResults> search = milvusClient.search(searchParam);
    Assert.assertEquals(search.getStatus().intValue(), 0);
    // delete
    R<MutationResult> delete =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(collectionName)
                .withExpr("book_id in [1,2,3]")
                .build());
    Assert.assertEquals(delete.getStatus().intValue(), 0);
    // release
    R<RpcStatus> rpcStatusR =
        milvusClient.releaseCollection(
            ReleaseCollectionParam.newBuilder().withCollectionName(collectionName).build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    // dropIndex
    R<RpcStatus> rpcStatusR1 =
        milvusClient.dropIndex(
            DropIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(rpcStatusR1.getStatus().intValue(), 0);
    // dropCollection
    R<RpcStatus> rpcStatusR2 =
        milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
    Assert.assertEquals(rpcStatusR2.getStatus().intValue(), 0);
    // import
    // ****

  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "db_admin with no privilege verify")
  public void adAdminWithNoPrivilegeVerify() {
    // createCredential
    R<RpcStatus> test001 =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder()
                .withUsername("test001")
                .withPassword("Lyp0107!")
                .build());
    logger.info("createCredential:" + test001);
    Assert.assertEquals(test001.getStatus().intValue(), -3);
    Assert.assertTrue(test001.getMessage().contains("permission deny"));
    // GetCollectionStatistics
    /*    R<GetCollectionStatisticsResponse> collectionStatistics = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withFlush(false).build());
    Assert.assertEquals(collectionStatistics.getStatus().intValue(), -3);
    Assert.assertTrue(collectionStatistics.getMessage().contains("permission deny"));*/
  }
}
