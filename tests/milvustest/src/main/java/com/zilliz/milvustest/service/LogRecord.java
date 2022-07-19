package com.zilliz.milvustest.service;

import org.apache.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogRecord implements ITestListener {
  Logger logger = Logger.getLogger(LogRecord.class);

  @Override
  public void onTestStart(ITestResult result) {
    logger.info("before test: " + result.getName() + " ,start time: " + this.getCurrentTime());
  }

  public String getCurrentTime() {
    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS");
    Date dt = new Date();
    return dateFormat.format(dt);
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    logger.info("run success: " + result.getName() + " ,run time:" + this.getCurrentTime());
  }

  @Override
  public void onTestFailure(ITestResult result) {
    logger.error("run failed: " + result.getName() + " ,run time:" + this.getCurrentTime());
    logger.error(result.getMethod() + " parameters：" + result.getParameters());
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    logger.info("skip test: " + result.getName() + " ,run time: " + this.getCurrentTime());
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {}

  @Override
  public void onStart(ITestContext context) {
    logger.info("start suit test: " + context.getName() + " ,run time: " + this.getCurrentTime());
  }

  @Override
  public void onFinish(ITestContext context) {
    logger.info("finish suit test: " + context.getName() + " ,run time: " + this.getCurrentTime());
  }
}
