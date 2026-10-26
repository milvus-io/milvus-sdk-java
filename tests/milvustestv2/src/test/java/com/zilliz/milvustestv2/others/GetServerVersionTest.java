package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetServerVersionTest extends BaseTest {

    @Test(description = "get server version", groups = {"L2"})
    public void getServerVersion() {
        String serverVersion = milvusClientV2.getServerVersion();
        // 3.0 nightly 的版本格式如 "3.0-20261023-6376134d4d"（无 v 前缀），放宽为包含数字版本号
        Assert.assertTrue(serverVersion != null && serverVersion.matches(".*\\d+\\.\\d+.*"),
                "server version should contain a numeric version: " + serverVersion);
    }
}
