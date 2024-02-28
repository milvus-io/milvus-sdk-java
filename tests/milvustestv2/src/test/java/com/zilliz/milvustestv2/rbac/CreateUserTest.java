package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 17:53
 */
public class CreateUserTest extends BaseTest {

    @AfterClass(alwaysRun = true)
    public void deleteUser(){
        milvusClientV2.dropUser(DropUserReq.builder().userName(CommonData.userName).build());
    }

    @Test(description = "create user ",groups = {"Smoke"})
    public void createUserTest(){
        milvusClientV2.createUser(CreateUserReq.builder()
                        .userName(CommonData.userName)
                        .password(CommonData.password)
                .build());
        List<String> strings = milvusClientV2.listUsers();
        Assert.assertTrue(strings.contains(CommonData.userName));
    }
}
