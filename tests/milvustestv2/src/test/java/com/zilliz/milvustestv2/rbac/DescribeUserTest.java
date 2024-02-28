package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 16:11
 */
public class DescribeUserTest extends BaseTest {

    @Test(description = "Describe user", groups = {"Smoke"})
    public void describeUserTest() {
        DescribeUserResp describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder().userName(CommonData.rootUser).build());
        System.out.println(describeUserResp);
    }
}
