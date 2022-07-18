package com.zilliz.milvustest.connection;

import com.zilliz.milvustest.common.BaseTest;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2022/7/11 14:14
 */
public class CloseTest extends BaseTest {
    @Test(description = "close connection")
    @Severity(SeverityLevel.BLOCKER)
    public void closeConnection(){
        milvusClient.close();
    }
}
