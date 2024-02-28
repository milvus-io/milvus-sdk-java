package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 18:27
 */
public class DropUserTest extends BaseTest {

    @BeforeClass(alwaysRun = true)
    public void createUser(){
        milvusClientV2.createUser(CreateUserReq.builder().userName(CommonData.userName).password(CommonData.password).build());
    }

   @Test(description = "drop user",groups = {"Smoke"})
    public void deleteUser(){
        milvusClientV2.dropUser(DropUserReq.builder().userName(CommonData.userName).build());
    }
}
