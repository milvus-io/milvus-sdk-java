package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 18:08
 */
public class UpdatePasswordTest extends BaseTest {

    @BeforeClass(alwaysRun = true)
    public void createUser(){
        milvusClientV2.createUser(CreateUserReq.builder().userName(CommonData.userName).password(CommonData.password).build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteUser(){
        milvusClientV2.dropUser(DropUserReq.builder().userName(CommonData.userName).build());
    }

    @Test(description = "update password")
    public void updatePassword(){
        milvusClientV2.updatePassword(UpdatePasswordReq.builder().userName(CommonData.userName).password(CommonData.password).newPassword("newPassword").build());
    }

    @Test(description = "update password use error old password",dependsOnMethods = {"updatePassword"})
    public void updatePWUseErrorOldPw(){
        try {
            milvusClientV2.updatePassword(UpdatePasswordReq.builder().userName(CommonData.userName).password(CommonData.password).newPassword("newPassword").build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("old password not correct"));
        }
    }

}
