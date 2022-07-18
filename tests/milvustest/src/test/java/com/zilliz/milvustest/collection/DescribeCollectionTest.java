package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.response.DescCollResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("DescribeCollection")
public class DescribeCollectionTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "query the name and schema of the collection")
  public void describeCollectionTest() {
    R<DescribeCollectionResponse> respDescribeCollection =
        milvusClient.describeCollection( // Return the name and schema of the collection.
            DescribeCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    System.out.println(respDescribeCollection);
    Assert.assertEquals(respDescribeCollection.getStatus().intValue(), 0);
    DescribeCollectionResponse respDescribeCollectionData = respDescribeCollection.getData();
    DescCollResponseWrapper descCollResponseWrapper =
        new DescCollResponseWrapper(respDescribeCollectionData);
    System.out.println(descCollResponseWrapper);
    Assert.assertEquals(descCollResponseWrapper.getCollectionName(), CommonData.defaultCollection);
    Assert.assertEquals(descCollResponseWrapper.getFields().size(), 3);
  }
}
