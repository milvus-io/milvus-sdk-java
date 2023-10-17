package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
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
@Feature("DropCollection")
public class DropCollectionTest extends BaseTest {
  public String commonCollection;

  @BeforeClass(description = "Create collection before test",alwaysRun=true)
  public void provideCollectionName() {
    commonCollection = CommonFunction.createNewCollection();
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "drop collection",groups = {"Smoke"})
  public void dropCollection() {
    DropCollectionParam build =
        DropCollectionParam.newBuilder().withCollectionName(commonCollection).build();
    R<RpcStatus> rpcStatusR = milvusClient.dropCollection(build);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop illegal collection")
  public void dropIllegalCollection() {
    String collection = "collection_" + MathUtil.getRandomString(10);
    DropCollectionParam build =
        DropCollectionParam.newBuilder().withCollectionName(collection).build();
    R<RpcStatus> rpcStatusR = milvusClient.dropCollection(build);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "DescribeCollection failed: can't find collection: " + collection + "");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "query after drop collection", dependsOnMethods = "dropCollection")
  public void queryAfterDropCollection() {
    milvusClient.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(commonCollection)
                    .withOutFields(outFields)
                    .withExpr(SEARCH_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
            queryResultsR.getException().getMessage(),"DescribeCollection failed: can't find collection: "+commonCollection);
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search after drop collection", dependsOnMethods = "dropCollection")
  public void searchAfterDropCollection() {
    milvusClient.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(commonCollection).build());
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(commonCollection)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
            searchResultsR.getException().getMessage(),"DescribeCollection failed: can't find collection: "+commonCollection);
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "hasCollection after drop collection", dependsOnMethods = "dropCollection")
  public void hasCollectionAfterDropCollection() {
    R<Boolean> booleanR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(commonCollection).build());
    Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    Assert.assertFalse(booleanR.getData());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Describe Collection after drop alias", dependsOnMethods = "dropCollection")
  public void describeCollectionAfterDropAlias() {
    R<DescribeCollectionResponse> describeCollectionResponseR =
            milvusClient.describeCollection(
                    DescribeCollectionParam.newBuilder().withCollectionName(commonCollection).build());
    Assert.assertEquals(describeCollectionResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(describeCollectionResponseR.getException().getMessage().contains("can't find collection"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop empty collection",expectedExceptions = ParamException.class)
  public void dropEmptyCollection() {
    DropCollectionParam build =
            DropCollectionParam.newBuilder().withCollectionName("").build();
    R<RpcStatus> rpcStatusR = milvusClient.dropCollection(build);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);

  }
}
