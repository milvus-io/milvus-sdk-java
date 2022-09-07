package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Collection")
@Feature("ReleaseCollection")
public class ReleaseCollectionTest extends BaseTest {
  @BeforeClass(description = "load collection",alwaysRun=true)
  public void loadCollection() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "release collection",groups = {"Smoke"})
  public void releaseCollection() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releaseCollection(
            ReleaseCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "query after release collection", dependsOnMethods = "releaseCollection")
  public void queryAfterDropCollection() {
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withOutFields(outFields)
                    .withExpr(SEARCH_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
            queryResultsR.getException().getMessage().contains("checkIfLoaded failed when query"));
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search after drop collection", dependsOnMethods = "releaseCollection")
  public void searchAfterDropCollection() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
            searchResultsR.getException().getMessage().contains("checkIfLoaded failed when search"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "release collection after load partition")
  public void releaseCollectionAfterLoadPartition() {
    milvusClient.loadPartitions(
        LoadPartitionsParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(Arrays.asList(CommonData.defaultPartition))
            .build());
    R<RpcStatus> rpcStatusR =
        milvusClient.releaseCollection(
            ReleaseCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
