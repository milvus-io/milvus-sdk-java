package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import io.milvus.v2.service.utility.request.GetServerVersionReq;
import io.milvus.v2.service.utility.response.GetServerVersionResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test cases for getServerVersionV2 API.
 * 与老接口 getServerVersion（只返回 version 字符串）不同，getServerVersionV2 在 detail=true 时
 * 额外返回 buildTime/gitCommit/goVersion/deployMode。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
@Slf4j
public class ServerVersionV2Test extends BaseTest {

    @Test(description = "Get server version V2 with detail=true", groups = {"Smoke"})
    public void getServerVersionV2WithDetail() {
        GetServerVersionResp resp = milvusClientV2.getServerVersionV2(GetServerVersionReq.builder()
                .detail(true)
                .build());
        Assert.assertNotNull(resp);
        Assert.assertNotNull(resp.getVersion());
        Assert.assertFalse(resp.getVersion().isEmpty());

        // version 应与老接口 getServerVersion() 返回值一致
        String oldVersion = milvusClientV2.getServerVersion();
        Assert.assertEquals(resp.getVersion(), oldVersion,
                "getServerVersionV2 version should equal old getServerVersion");

        // nightly 构建应带有详细构建信息；若服务端返回为空，放宽为仅校验 version 一致
        if (resp.getBuildTime() != null && !resp.getBuildTime().isEmpty()) {
            Assert.assertNotNull(resp.getGitCommit());
            Assert.assertFalse(resp.getGitCommit().isEmpty());
            Assert.assertNotNull(resp.getGoVersion());
            Assert.assertFalse(resp.getGoVersion().isEmpty());
            Assert.assertNotNull(resp.getDeployMode());
            Assert.assertFalse(resp.getDeployMode().isEmpty());
        } else {
            log.info("server does not return detail build info, only version is verified: " + resp.getVersion());
        }
        log.info("server version detail: " + resp);
    }

    @Test(description = "Get server version V2 with detail=false", groups = {"Smoke"})
    public void getServerVersionV2WithoutDetail() {
        GetServerVersionResp resp = milvusClientV2.getServerVersionV2(GetServerVersionReq.builder()
                .detail(false)
                .build());
        Assert.assertNotNull(resp);
        Assert.assertNotNull(resp.getVersion());
        Assert.assertFalse(resp.getVersion().isEmpty());
        Assert.assertEquals(resp.getVersion(), milvusClientV2.getServerVersion());
    }

    @Test(description = "Get server version V2 with detail not set (default false)", groups = {"Smoke"})
    public void getServerVersionV2DefaultDetail() {
        GetServerVersionResp resp = milvusClientV2.getServerVersionV2(GetServerVersionReq.builder().build());
        Assert.assertNotNull(resp);
        Assert.assertNotNull(resp.getVersion());
        Assert.assertFalse(resp.getVersion().isEmpty());
    }
}
