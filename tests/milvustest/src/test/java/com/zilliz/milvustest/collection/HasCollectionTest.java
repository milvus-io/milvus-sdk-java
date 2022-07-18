package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.collection.HasCollectionParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("HasCollection")
public class HasCollectionTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Check collection is existent")
  public void hasCollectionTest() {
    R<Boolean> respHasCollection =
        milvusClient.hasCollection(
            HasCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    Assert.assertEquals(respHasCollection.getData(), Boolean.TRUE);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Check collection is not existent")
  public void hasNoExistentCollectionTest() {
    R<Boolean> respHasCollection =
        milvusClient.hasCollection(
            HasCollectionParam.newBuilder()
                .withCollectionName("collection" + MathUtil.getRandomString(10))
                .build());
    Assert.assertEquals(respHasCollection.getData(), Boolean.FALSE);
  }
}
