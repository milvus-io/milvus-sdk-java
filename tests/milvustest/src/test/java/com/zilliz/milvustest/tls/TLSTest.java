package com.zilliz.milvustest.tls;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CheckHealthResponse;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.highlevel.dml.InsertRowsParam;
import io.milvus.param.highlevel.dml.SearchSimpleParam;
import io.milvus.param.highlevel.dml.response.InsertResponse;
import io.milvus.param.highlevel.dml.response.SearchResponse;
import io.milvus.param.index.CreateIndexParam;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author yongpeng.li
 * @Date 2023/8/3 18:17
 */
public class TLSTest {
    @Test
    public void oneWayAuth(){
        String path="../milvustest/src/test/java/resources/tls";
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(System.getProperty("milvusHost") == null
                        ? PropertyFilesUtil.getRunValue("milvusHost")
                        : System.getProperty("milvusHost"))
                .withPort(Integer.parseInt(
                        System.getProperty("milvusPort") == null
                                ? PropertyFilesUtil.getRunValue("milvusPort")
                                : System.getProperty("milvusPort")))
                .withServerName("localhost")
                .withServerPemPath(path+"/server.pem")
                .withSecure(true)
                .build();
        MilvusServiceClient milvusClient = new MilvusServiceClient(connectParam);

        R<CheckHealthResponse> health = milvusClient.checkHealth();
        if (health.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(health.getMessage());
        } else {
            System.out.println(health);
        }
    }

    @Test
    private static void twoWayAuth() throws InterruptedException {
        String path = "../milvustest/src/test/java/resources/tls";
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(System.getProperty("milvusHost") == null
                        ? PropertyFilesUtil.getRunValue("milvusHost")
                        : System.getProperty("milvusHost"))
                .withPort(Integer.parseInt(
                        System.getProperty("milvusPort") == null
                                ? PropertyFilesUtil.getRunValue("milvusPort")
                                : System.getProperty("milvusPort")))
                .withServerName("localhost")
                .withCaPemPath(path + "/ca.pem")
                .withClientKeyPath(path + "/client.key")
                .withClientPemPath(path + "/client.pem")
                .withSecure(true)
                .build();
        MilvusServiceClient milvusClient = new MilvusServiceClient(connectParam);

        R<CheckHealthResponse> health = milvusClient.checkHealth();
        if (health.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(health.getMessage());
        } else {
            System.out.println(health);
        }
        // create collection
        String collectionName = "Collection_" + MathUtil.getRandomString(10);
        FieldType fieldType1 =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType fieldType2 =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
        FieldType fieldType3 =
                FieldType.newBuilder()
                        .withName(CommonData.defaultVectorField)
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        CreateCollectionParam createCollectionReq =
                CreateCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withDescription("Test" + collectionName + "search")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .addFieldType(fieldType3)
                        .build();
        R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
        // insert data
        List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(10000);
        R<InsertResponse> insert = milvusClient.insert(InsertRowsParam.newBuilder()
                .withCollectionName(collectionName)
                .withRows(jsonObjects)
                .build());

        // build index
        milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(fieldType3.getName())
                .withMetricType(MetricType.L2)
                .withIndexType(IndexType.HNSW)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withIndexName("index_def").build());
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName).build());
        //search
        for(int i = 0; i < 100; i++) {
          //
        System.out.println("第"+i+"次查询");
        Thread.sleep(1000);
        List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
        R<SearchResponse> search = milvusClient.withRetry(10).withRetryInterval(1, TimeUnit.SECONDS)
                .search(SearchSimpleParam.newBuilder()
                .withCollectionName(collectionName)
                .withOffset(0L)
                .withLimit(100L)
                .withFilter("book_id>5000")
                .withVectors(search_vectors)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(search.getStatus().intValue(), R.Status.Success.getCode());
        }
    }
}
