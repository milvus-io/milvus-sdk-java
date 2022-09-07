package com.zilliz.milvustest.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class CustomerListener extends TestListenerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(CustomerListener.class);
  /**
   * 开始
   *
   * @param iTestContext ITestContext
   */
  @Override
  public void onStart(ITestContext iTestContext) {
    super.onStart(iTestContext);
    logger.info("===================={}测试开始====================", iTestContext.getName());
  }

  /**
   * 测试开始
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestStart(ITestResult iTestResult) {
    super.onTestStart(iTestResult);
    logger.info("========{}测试开始========", iTestResult.getName());
  }

  /**
   * 测试成功
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestSuccess(ITestResult iTestResult) {
    super.onTestSuccess(iTestResult);
    logger.info("========{}测试通过========", iTestResult.getName());
  }

  /**
   * 测试失败
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestFailure(ITestResult iTestResult) {
    super.onTestFailure(iTestResult);
    logger.error("========{}测试失败,失败原因如下：\n{}========", iTestResult.getName(), iTestResult.getThrowable());

  }

  /**
   * 测试跳过
   *
   * @param iTestResult ITestResult
   */
  @Override
  public void onTestSkipped(ITestResult iTestResult) {
    super.onTestSkipped(iTestResult);
    logger.info("========{}跳过测试========", iTestResult.getName());
  }

  /**
   * 结束
   *
   * @param iTestContext ITestContext
   */
  @Override
  public void onFinish(ITestContext iTestContext) {
    super.onFinish(iTestContext);
    logger.info("===================={}测试结束====================", iTestContext.getName());
  }
}
