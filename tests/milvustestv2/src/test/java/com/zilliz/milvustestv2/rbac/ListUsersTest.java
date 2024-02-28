package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 16:08
 */
public class ListUsersTest extends BaseTest {
    @Test(description = "list users", groups = {"Smoke"})
    public void listUserTest() {
        List<String> strings = milvusClientV2.listUsers();
        Assert.assertTrue(strings.contains(CommonData.rootUser));
    }
}
