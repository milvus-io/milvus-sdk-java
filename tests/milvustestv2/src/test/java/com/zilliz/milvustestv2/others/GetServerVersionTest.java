package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetServerVersionTest extends BaseTest {

    @Test(description = "get server version", groups = {"L2"})
    public void getServerVersion() {
        String serverVersion = milvusClientV2.getServerVersion();
        Assert.assertTrue(serverVersion.contains("v"));
    }
}
