package com.zilliz.milvustest.service;

import org.apache.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class ComstumerListener extends TestListenerAdapter {
  Logger logger = Logger.getLogger(ComstumerListener.class);
  /**
   * 开始
   *
   * @param iTestContext ITestContext
   */
  @Override
  public void onStart(ITestContext iTestContext) {
    super.onStart(iTestContext);
    logger.info(
        String.format("====================[%s]测试开始====================", iTestContext.getName()));
  }

  /**
   * 测试开始
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestStart(ITestResult iTestResult) {
    super.onTestStart(iTestResult);
    logger.info(String.format("========%s测试开始========", iTestResult.getName()));
  }

  /**
   * 测试成功
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestSuccess(ITestResult iTestResult) {
    super.onTestSuccess(iTestResult);
    logger.info(String.format("========%s测试通过========", iTestResult.getName()));
  }

  /**
   * 测试失败
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestFailure(ITestResult iTestResult) {
    super.onTestFailure(iTestResult);
    logger.error(
        String.format(
            "========%s测试失败,失败原因如下：\n%s========",
            iTestResult.getName(), iTestResult.getThrowable()));

    /** 出现异常进行截图操作，这里得要自己去实现 */
  }

  /**
   * 测试跳过
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestSkipped(ITestResult iTestResult) {
    super.onTestSkipped(iTestResult);
    logger.info(String.format("========%s跳过测试========", iTestResult.getName()));
  }

  /**
   * 结束
   *
   * @param iTestContext ITestContext
   */
  @Override
  public void onFinish(ITestContext iTestContext) {
    super.onFinish(iTestContext);
    logger.info(
        String.format("====================%s测试结束====================", iTestContext.getName()));
  }
}
