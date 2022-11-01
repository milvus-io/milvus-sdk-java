package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author yongpeng.li
 * @Date 2022/10/13 09:30
 */
@Slf4j
public class LimitWriting {
    public static final MilvusServiceClient milvusClient =
            new MilvusServiceClient(
                    ConnectParam.newBuilder()
                            .withUri(
                                    System.getProperty("milvusUri") == null
                                            ? PropertyFilesUtil.getRunValue("milvusUri")
                                            : System.getProperty("milvusUri"))
                            .withAuthorization("db_admin", "Lyp0107!")
                            .build());

    @Test(description = "Test maxTimeTickDelay")
    public void ttProtectTest(){
        String collectionName = "book128";
        int dim = 128;
        FieldType bookIdField =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType wordCountField =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType bookIntroField =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(dim)
                        .build();
        CreateCollectionParam createCollectionParam =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test book search")
                        .withShardsNum(2)
                        .addFieldType(bookIdField)
                        .addFieldType(wordCountField)
                        .addFieldType(bookIntroField)
                        .build();
        R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
        log.info(collection.toString());
        // insert data with customized ids
        Random ran = new Random();
        int singleNum = 500000;
        int insertRounds = 10;
        double insertTotalTime = 0.00;
        for (int r = 0; r < insertRounds; r++) {
            List<Long> book_id_array = new ArrayList<>();
            List<Long> word_count_array = new ArrayList<>();
            List<List<Float>> book_intro_array = new ArrayList<>();
            for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
                book_id_array.add(i);
                word_count_array.add(i + 10000);
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                book_intro_array.add(vector);
            }
            List<InsertParam.Field> fields = new ArrayList<>();
            fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
            fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
            fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
            InsertParam insertParam =
                    InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
            long startTime = System.currentTimeMillis();
            R<MutationResult> insertR = milvusClient.insert(insertParam);
            log.info(insertR.getStatus().toString());
            long endTime = System.currentTimeMillis();
            insertTotalTime = (endTime - startTime) / 1000.00;
            log.info("------ insert " + singleNum + " entities cost " + insertTotalTime + " seconds");
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test(description = "Concurrent insert")
    public void insertConcurrency() {
        String newCollection = CommonFunction.createNewCollection();
        System.out.println("create collection " + newCollection + " successfully");
        int poolNum=10;
        ExecutorService executorService = Executors.newFixedThreadPool(poolNum);
        int singleNum = 10000;
        int insertRounds = 2000;

        List<InsertParam.Field> fields = CommonFunction.generateData(singleNum);
        InsertParam insertParam =
                InsertParam.newBuilder().withCollectionName(newCollection).withFields(fields).build();
        for (int e = 0; e < poolNum; e++) {
            int finalE = e;
            executorService.execute(
                    () -> {
                        for (int r = 0; r < insertRounds; r++) {
                            long insertTotalTime = 0L;
                            long startTime = System.currentTimeMillis();
                            R<MutationResult> insertR = milvusClient.insert(insertParam);
                            long endTime = System.currentTimeMillis();
                            insertTotalTime = (long) ((endTime - startTime) / 1000.0);
                            System.out.println(
                                    "Thread "
                                            + finalE
                                            + " insert "
                                            + singleNum
                                            + " entities cost "
                                            + insertTotalTime
                                            + " seconds");
                        }
                    });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn(e.getMessage());
        }
    }

    @Test(description = "Test maxTimeTickDelay")
    public void ttProtectTest2(){
        String collectionName = "book128";
        int dim = 128;
        FieldType bookIdField =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType wordCountField =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType bookIntroField =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(dim)
                        .build();
        CreateCollectionParam createCollectionParam =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test book search")
                        .withShardsNum(2)
                        .addFieldType(bookIdField)
                        .addFieldType(wordCountField)
                        .addFieldType(bookIntroField)
                        .build();
        R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
        log.info(collection.toString());
        // insert data with customized ids
        Random ran = new Random();
        int singleNum = 1000;
        int insertRounds = 1000;
        double insertTotalTime = 0.00;
        for (int r = 0; r < insertRounds; r++) {
            List<Long> book_id_array = new ArrayList<>();
            List<Long> word_count_array = new ArrayList<>();
            List<List<Float>> book_intro_array = new ArrayList<>();
            for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
                book_id_array.add(i);
                word_count_array.add(i + 10000);
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                book_intro_array.add(vector);
            }
            List<InsertParam.Field> fields = new ArrayList<>();
            fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
            fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
            fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
            InsertParam insertParam =
                    InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
            long startTime = System.currentTimeMillis();
            R<MutationResult> insertR = milvusClient.insert(insertParam);
            log.info(insertR.getStatus().toString());
            long endTime = System.currentTimeMillis();
            insertTotalTime = (endTime - startTime) / 1000.00;
            log.info("------ insert " + singleNum + " entities cost " + insertTotalTime + " seconds");
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test(description = "load")
    public void loadTest(){
        R<RpcStatus> loadR = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("book128").build());
        log.info("load:"+loadR);

    }

    @Test(description = "release")
    public  void release(){
        R<RpcStatus> book128 = milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
                .withCollectionName("book128").build());
        log.info("release:"+book128);
    }

    @Test(description = "hasCollection")
    public void hasCollection(){
        R<GetCollectionStatisticsResponse> book128 = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                .withCollectionName("book128")
                .withFlush(true).build());

    }

}
