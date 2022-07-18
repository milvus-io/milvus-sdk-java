/*
package com.zilliz.milvustest.businessflow;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.*;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.BulkloadParam;
import io.milvus.param.dml.GetBulkloadStateParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.response.GetCollStatResponseWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.qameta.allure.Issue;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

*/
/**
 * @Author yongpeng.li @Date 2022/6/2 2:00 PM
 *//*

   public class BigFileBulkLoadTest extends BaseTest {
       @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/301")
       @Test(description = "Big file bulk load test")
       public void BigFileBulkLoadTest1() throws IOException, NoSuchAlgorithmException, InvalidKeyException, InterruptedException {
           //create collection
           String newColltcion = CommonFunction.createNewCollection();
           milvusClient.createPartition(CreatePartitionParam.newBuilder().withCollectionName(newColltcion)
                   .withPartitionName(CommonData.defaultPartition)
                   .build());
           // create index
           milvusClient.createIndex(CreateIndexParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .withFieldName(CommonData.defaultVectorField)
                   .withIndexName(CommonData.defaultIndex)
                   .withMetricType(MetricType.L2)
                   .withIndexType(IndexType.IVF_FLAT)
                   .withExtraParam(CommonData.defaultExtraParam)
                   .withSyncMode(Boolean.FALSE)
                   .build());
           //generate json files
           String path = CommonData.defaultBulkLoadPath;
           List<FileBody> fileBodyList = CommonFunction.generateDefaultFileBody();
           int rows=5000;
           Boolean aBoolean = FileUtils.generateMultipleFiles(true, rows, 128, fileBodyList, true
                   , path, "bigJson", "json", 1);
           System.out.println(aBoolean);
           // upload json to minio
           FileUtils.fileUploader(path,"bigJson0.json","bigData");
           long startTime = System.currentTimeMillis();
           // bulk load
           R<ImportResponse> bulkload = milvusClient.bulkload(BulkloadParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .addFile("bigData/bigJson0.json").withRowBased(true)
                   .build());
           softAssert.assertEquals(bulkload.getStatus().intValue(),0);
           softAssert.assertEquals(bulkload.getData().getTasksCount(),1);
           ImportResponse data = bulkload.getData();
           Long taskId=data.getTasks(0);
           R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                   .withTaskID(taskId).build());
           softAssert.assertEquals(bulkloadState.getStatus().intValue(),0);
           // check state
           int i=0;
           while (i<100){
               R<GetImportStateResponse> reGetState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                       .withTaskID(taskId).build());
               GetImportStateResponse data1 = reGetState.getData();
               System.out.println("current state:"+data1.getStateValue()+"-"+data1.getState());
               if(data1.getStateValue()==5){
                   softAssert.assertEquals(data1.getRowCount(),rows);
                   long finish = System.currentTimeMillis();
                   long elapsedTime=finish-startTime;
                   System.out.println("Import big data file use:"+elapsedTime+"ms");
                   System.out.println("expected rows:"+rows+",actual rows:"+data1.getRowCount());
                   break;
               }
               Thread.sleep(5000);
               i++;
               if(i==100||data1.getStateValue()==1){
                   softAssert.assertEquals(data1.getStatus().toString(),"ImportPersisted","importFailed!!");
               }
           }
           Thread.sleep(5000);
           //query collection info
           R<GetCollectionStatisticsResponse> collectionStatistics = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .withFlush(true).build());
           softAssert.assertEquals(collectionStatistics.getStatus().intValue(),0);
           GetCollStatResponseWrapper getCollStatResponseWrapper = new GetCollStatResponseWrapper(collectionStatistics.getData());
           System.out.println("rows count of "+newColltcion+"is:"+getCollStatResponseWrapper.getRowCount());
           softAssert.assertEquals(getCollStatResponseWrapper.getRowCount(),rows);
           // load
           R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(newColltcion).build());
           softAssert.assertEquals(rpcStatusR.getStatus().intValue(),0);
           softAssert.assertEquals(rpcStatusR.getData().getMsg(),"Success");

           //search

           //query
           String query_Param = "book_id  in [2,4,6,8]";
           List<String> outFields=Arrays.asList("book_id","word_count");
           QueryParam queryParam= QueryParam.newBuilder()
                   .withCollectionName(newColltcion)
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
           //release
           milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(newColltcion).build());
           milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(newColltcion).build());
           softAssert.assertAll();
       }

       @Test(description = "Big json file bulk load test after create index and load")
       public void bigJsonFileBulkLoadTest2() throws InterruptedException {
           //create collection
           String newColltcion = CommonFunction.createNewCollection();
           milvusClient.createPartition(CreatePartitionParam.newBuilder().withCollectionName(newColltcion)
                   .withPartitionName(CommonData.defaultPartition)
                   .build());
           // create index
           milvusClient.createIndex(CreateIndexParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .withFieldName(CommonData.defaultVectorField)
                   .withIndexName(CommonData.defaultIndex)
                   .withMetricType(MetricType.L2)
                   .withIndexType(IndexType.IVF_FLAT)
                   .withExtraParam(CommonData.defaultExtraParam)
                   .withSyncMode(Boolean.FALSE)
                   .build());
           // load collection
           R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(newColltcion).withSyncLoad(false).build());
           softAssert.assertEquals(rpcStatusR.getStatus().intValue(),0);
           softAssert.assertEquals(rpcStatusR.getData().getMsg(),"Success");
           long startTime = System.currentTimeMillis();
           // bulk load
           int rows=5000;
           R<ImportResponse> bulkload = milvusClient.bulkload(BulkloadParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .addFile("bigData/bigJson0.json").withRowBased(true)
                   .build());
           softAssert.assertEquals(bulkload.getStatus().intValue(),0);
           softAssert.assertEquals(bulkload.getData().getTasksCount(),1);
           ImportResponse data = bulkload.getData();
           Long taskId=data.getTasks(0);
           R<GetImportStateResponse> bulkloadState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                   .withTaskID(taskId).build());
           softAssert.assertEquals(bulkloadState.getStatus().intValue(),0);
           // check state
           int i=0;
           while (i<100){
               R<GetImportStateResponse> reGetState = milvusClient.getBulkloadState(GetBulkloadStateParam.newBuilder()
                       .withTaskID(taskId).build());
               GetImportStateResponse data1 = reGetState.getData();
               System.out.println("current state:"+data1.getStateValue()+"-"+data1.getState());
               if(data1.getStateValue()==5){
                   softAssert.assertEquals(data1.getRowCount(),rows);
                   long finish = System.currentTimeMillis();
                   long elapsedTime=finish-startTime;
                   System.out.println("Import big data file use:"+elapsedTime+"ms");
                   System.out.println("expected rows:"+rows+",actual rows:"+data1.getRowCount());
                   break;
               }
               Thread.sleep(5000);
               i++;
               if(i==100||data1.getStateValue()==1){
                   softAssert.assertEquals(data1.getStatus().toString(),"ImportPersisted","importFailed!!");
               }
           }
           Thread.sleep(120000);
           //query collection info
           R<GetCollectionStatisticsResponse> collectionStatistics = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                   .withCollectionName(newColltcion)
                   .withFlush(true).build());
           softAssert.assertEquals(collectionStatistics.getStatus().intValue(),0);
           GetCollStatResponseWrapper getCollStatResponseWrapper = new GetCollStatResponseWrapper(collectionStatistics.getData());
           System.out.println("rows count of "+newColltcion+"is:"+getCollStatResponseWrapper.getRowCount());
           softAssert.assertEquals(getCollStatResponseWrapper.getRowCount(),rows);
           //query
           String query_Param = "book_id  in [2,4,6,8]";
           List<String> outFields=Arrays.asList("book_id","word_count");
           QueryParam queryParam= QueryParam.newBuilder()
                   .withCollectionName(newColltcion)
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
           //release
           milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(newColltcion).build());
           milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(newColltcion).build());
           softAssert.assertAll();
       }


   }
   */
