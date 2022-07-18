/*
package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.util.FileUtils;
import io.milvus.grpc.GetImportStateResponse;
import io.milvus.grpc.ImportResponse;
import io.milvus.grpc.ListImportTasksResponse;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.BulkloadParam;
import io.milvus.param.dml.GetBulkloadStateParam;
import io.milvus.param.dml.ListBulkloadTasksParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.response.QueryResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Issue;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GetBulkloadStateTest extends BaseTest {
    @DataProvider(name = "singleRowBasedTaskId")
    public Object[][] singleRowBasedBulkLoad(){
        Object[][] objects=new Object[1][1];
        R<ImportResponse> importResponseR=milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("rowJson0.json")
                .withRowBased(true)
                .build());
        ImportResponse data = importResponseR.getData();
        Optional.ofNullable(data).ifPresent(x-> objects[0][0]=x.getTasks(0));
        return objects;
    }

    @DataProvider(name = "singleColBasedTaskId")
    public Object[][] singleColBasedBulkLoad(){
        Object[][] objects=new Object[1][1];
        R<ImportResponse> importResponseR=milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("colJson0.json")
                .withRowBased(false)
                .build());
        ImportResponse data = importResponseR.getData();
        Optional.ofNullable(data).ifPresent(x-> objects[0][0]=x.getTasks(0));
        return objects;
    }

    @DataProvider(name = "bigRowJsomTaskId")
    public Object[][] bigRowBasedTask() throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        String path = CommonData.defaultBulkLoadPath;
        List<FileBody> fileBodyList = CommonFunction.generateDefaultFileBody();
        Boolean aBoolean = FileUtils.generateMultipleFiles(Boolean.TRUE, 1000, 128, fileBodyList, true
                , path, "bigJson", "json", 1);
        System.out.println(aBoolean);
        FileUtils.fileUploader(path,"bigJson0.json",null);
        R<ImportResponse> bulkload = milvusClient.bulkload(BulkloadParam.newBuilder().withCollectionName(CommonData.defaultCollection)
                .addFile("bigJson0.json")
                .build());
        ImportResponse data = bulkload.getData();
        return new Object[][]{{data.getTasksList().get(0)}};

    }

    @Epic("L0")
    @Test(description = "Get bulk load state of  single row based json task",dataProvider = "singleRowBasedTaskId")
    public void getSingleRowBaseJsonState(Long taskId){
        R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                .withTaskID(taskId).build());
        Assert.assertEquals(bulkloadState.getStatus().intValue(),0);
        Assert.assertEquals(bulkloadState.getData().getInfosList().size(),1);
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
        String query_Param = "book_id in [2,4,6,8]";
        List<String> outFields= Arrays.asList("book_id","word_count");
        QueryParam queryParam= QueryParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withOutFields(outFields)
                .withExpr(query_Param)
                .build();
        R<QueryResults> queryResultsR = milvusClient.query(queryParam);
        QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
        softAssert.assertEquals(queryResultsR.getStatus().intValue(),0);
        softAssert.assertEquals(queryResultsR.getData().getFieldsDataCount(),2);
        softAssert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(),4);
        System.out.println("query book_id:"+wrapperQuery.getFieldWrapper("book_id").getFieldData());
        System.out.println("query word_count:"+wrapperQuery.getFieldWrapper("word_count").getFieldData());
    }
    @Epic("L0")
    @Test(description = "Get bulk load state of  single column based json task",dataProvider = "singleColBasedTaskId")
    public void getSingleColBaseJsonState(Long taskId){
        R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                .withTaskID(taskId).build());
        System.out.println(bulkloadState);
        Assert.assertEquals(bulkloadState.getStatus().intValue(),0);
        Assert.assertEquals(bulkloadState.getData().getInfosList().size(),1);
    }

    @Epic("L0")
    @Test(description = "big row based json bulk load test",dataProvider = "bigRowJsomTaskId")
    public void getBigRowBasedJsonState(Long taskId){
        R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                .withTaskID(taskId).build());
        Assert.assertEquals(bulkloadState.getStatus().intValue(),0);
        Assert.assertEquals(bulkloadState.getData().getInfosList().size(),1);
    }

    @Epic("L0")
    @Test(description = "get bulk load state of multiple row based json")
    public void getMultiRowBasedJsonState(){
        List<String> fileNames = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fileNames.add(CommonData.defaultRowJson + i + ".json");
        }
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(fileNames)
                .build());
        List<Long> tasksList = importResponseR.getData().getTasksList();
        // multiple thread
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < tasksList.size(); i++) {
            int finalI = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                            .withTaskID(tasksList.get(finalI)).build());
                    System.out.println("execute thread"+finalI);
                    System.out.println(bulkloadState);
                    Assert.assertEquals(bulkloadState.getStatus().intValue(),0);
                    Assert.assertEquals(bulkloadState.getData().getInfosList().size(),1);
                }
            });
        }
        executorService.shutdown();


    }


}
*/
