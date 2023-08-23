package com.zilliz.milvustest.service;


import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.util.HttpClientUtils;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.text.DecimalFormat;
import java.time.LocalDate;

public class CustomerListener extends TestListenerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(CustomerListener.class);
  public int totalCase=0;
  public int passCase=0;
  public long startTime ;
  public long endTime ;
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
    // insert result into db
    double passRate= iTestContext.getPassedTests().size()*100.00 / (iTestContext.getPassedTests().size()+iTestContext.getFailedTests().size()+iTestContext.getSkippedTests().size());
    int costTime= (int) ((iTestContext.getEndDate().getTime()-iTestContext.getStartDate().getTime())/1000/60);
    String scenarioDesc=System.getProperty("ScenarioDesc") == null
            ? PropertyFilesUtil.getRunValue("ScenarioDesc")
            : System.getProperty("ScenarioDesc");
    String BuildId=System.getProperty("BuildId") == null
            ? PropertyFilesUtil.getRunValue("BuildId")
            : System.getProperty("BuildId");
    String SDKBranch=System.getProperty("SDKBranch") == null
            ? PropertyFilesUtil.getRunValue("SDKBranch")
            : System.getProperty("SDKBranch");
    String jenkinsLink="https://qa-jenkins.milvus.io/job/Java-sdk-test-nightly/"+BuildId+"/";
    String githubLink="https://github.com/milvus-io/milvus-sdk-java/actions/workflows/java_sdk_ci_test.yaml";
    JSONObject request=new JSONObject();
    request.put("Product","Milvus");
    request.put("Category","Function");
    request.put("Date", LocalDate.now().toString());
    request.put("Scenario",scenarioDesc);
    request.put("Branch",SDKBranch);
    request.put("ImageName","2.2.0");
    request.put("SDK","java");
    request.put("MilvusMode","standalone");
    request.put("MqMode","rocksMq");
    request.put("TestResult",passRate==100?"pass":"fail");
    request.put("PassRate", passRate);
    request.put("RunningTime", costTime);
    request.put("Link",scenarioDesc.equalsIgnoreCase("CI")?githubLink:jenkinsLink);
    String s = HttpClientUtils.doPostJson("http://qtp-server.zilliz.cc/results/insert",request.toJSONString());
    logger.info("insert result:"+s);
/*    if (iTestContext.getFailedTests().size()>0){
      throw new RuntimeException("Case Failed "+iTestContext.getFailedTests().size());
    }*/
  }
}
