package com.zilliz.milvustestv2.listener;


import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.utils.HttpClientUtils;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.time.LocalDate;

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
    JsonObject request=new JsonObject();
    request.addProperty("Product","Milvus");
    request.addProperty("Category","Function");
    request.addProperty("Date", LocalDate.now().toString());
    request.addProperty("Scenario",scenarioDesc);
    request.addProperty("Branch",SDKBranch);
    request.addProperty("ImageName","2.2.0");
    request.addProperty("SDK","java");
    request.addProperty("MilvusMode","standalone");
    request.addProperty("MqMode","rocksMq");
    request.addProperty("TestResult",passRate==100?"pass":"fail");
    request.addProperty("PassRate", passRate);
    request.addProperty("RunningTime", costTime);
    request.addProperty("Link",scenarioDesc.equalsIgnoreCase("CI")?githubLink:jenkinsLink);
    String s = HttpClientUtils.doPostJson("http://qtp-server.zilliz.cc/results/insert",request.toString());
    logger.info("insert result:"+s);
/*    if (iTestContext.getFailedTests().size()>0){
      throw new RuntimeException("Case Failed "+iTestContext.getFailedTests().size());
    }*/
  }
}
