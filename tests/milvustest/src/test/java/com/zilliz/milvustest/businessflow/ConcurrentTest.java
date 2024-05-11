package com.zilliz.milvustest.businessflow;

import com.google.gson.*;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.role.*;
import io.milvus.response.SearchResultsWrapper;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author yongpeng.li
 * @Date 2023/5/5 11:27
 */
@Slf4j

public class ConcurrentTest {
     int THREAD=System.getProperty("thread") == null?10: Integer.parseInt(System.getProperty("thread"));
     int dataNum=System.getProperty("dataNum") == null?10000: Integer.parseInt(System.getProperty("dataNum"));
     int searchNum=System.getProperty("searchNum") == null?10: Integer.parseInt(System.getProperty("searchNum"));
     int TopK=System.getProperty("TopK") == null?2: Integer.parseInt(System.getProperty("TopK"));
     int nprobe=System.getProperty("nprobe") == null?10: Integer.parseInt(System.getProperty("nprobe"));
     int nq=System.getProperty("nq") == null?10: Integer.parseInt(System.getProperty("nq"));
     String host=System.getProperty("host") == null?"10.102.9.108": System.getProperty("host");
     int port=System.getProperty("port") == null?19530: Integer.parseInt(System.getProperty("port"));
    Object[][] objects=new Object[][]{};
    @DataProvider(name = "UserInfo")
    public Object[][] provideUser(){
        String[][] userinfo=new String[THREAD][2];
        for(int i = 0; i < THREAD; i++) {
         userinfo[i][0]="Username"+i;
         userinfo[i][1]="Password"+i;
        }
        return userinfo;
    }
    /*@DataProvider(name = "providerPrivilegeList")
    public Object[][] providerPrivilegeList() {
        File jsonFile=new File("./src/test/java/resources/testdata/privilege.json");
        String str = FileUtils.getStr(jsonFile);
        JsonArray jsonArray = new Gson().fromJson(str, JsonArray.class);
        objects=new Object[jsonArray.size()][3];
        for (int i = 0; i < jsonArray.size(); i++) {
            objects[i][0]=jsonArray.get(i).getAsJsonObject.get("object").getAsString();
            objects[i][1]=jsonArray.get(i)).getAsJsonObject.get("objectName").getAsString();
            objects[i][2]=jsonArray.get(i)).getAsJsonObject.get("privilege").getAsString();
        }

        return objects;
    }*/

    @AfterClass()
    public void cleanTestData(){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        for(int i = 0; i < THREAD; i++) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName("collection"+i).build());
            milvusClient.deleteCredential(DeleteCredentialParam.newBuilder().withUsername("Username"+i).build());
        }
    }

    @Test(dataProvider = "UserInfo")
    public void registerUserInfo(String username,String password){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        R<RpcStatus> credential = milvusClient.createCredential(CreateCredentialParam.newBuilder().withUsername(username).withPassword(password).build());
        log.info(String.valueOf(credential.getStatus()));
        log.info(credential.getData().toString());

    }

    /*@Test(description = "Create role",dataProvider = "providerPrivilegeList",dependsOnMethods = "registerUserInfo")
    public void createAndGrantRole(String object,String objectName,String privilege) {
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        R<RpcStatus> role =
                milvusClient.createRole(
                        CreateRoleParam.newBuilder().withRoleName("newRole").build());
        Assert.assertEquals(role.getStatus().intValue(), 0);
        Assert.assertEquals(role.getData().getMsg(), "Success");
        R<RpcStatus> rpcStatusR =
                milvusClient.grantRolePrivilege(
                        GrantRolePrivilegeParam.newBuilder()
                                .withRoleName("newRole")
                                .withObject(object)
                                .withObjectName(objectName)
                                .withPrivilege(privilege)
                                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
        milvusClient.close();
    }*/

    @Test(dataProvider = "UserInfo",dependsOnMethods = "registerUserInfo")
    public void addUserToRole(String username,String password){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        R<RpcStatus> rpcStatusR =
                milvusClient.addUserToRole(
                        AddUserToRoleParam.newBuilder()
                                .withUserName(username)
                                .withRoleName("admin")
                                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
        milvusClient.close();
    }

    @Test(dependsOnMethods = "addUserToRole")
    public void createCollection() throws ExecutionException, InterruptedException {
        int threads=THREAD;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username"+finalE,"Password"+finalE)
                                        .build());
            // 创建collection
                String collectionName="collection"+finalE;
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
                                .withName("book_intro")
                                .withDataType(DataType.FloatVector)
                                .withDimension(128)
                                .build();
                CreateCollectionParam createCollectionReq =
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(collectionName)
                                .withDescription("Test " + collectionName + " search")
                                .withShardsNum(2)
                                .addFieldType(fieldType1)
                                .addFieldType(fieldType2)
                                .addFieldType(fieldType3)
                                .build();
                R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
                log.info("线程"+finalE+":用户Username"+finalE+"创建collection："+collectionName);
                log.info(String.valueOf(collection.getStatus()));
                milvusClient.close();
                return collection;
            };
            Future future = executorService.submit(callable);
            list.add(future);
    }
        for (Future future : list) {
            log.info("运行结果:"+future.get().toString());
        }
        executorService.shutdown();

    }

    @Test(dependsOnMethods = "createCollection")
    public void insertData() throws ExecutionException, InterruptedException {
        int threads=THREAD;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username"+finalE,"Password"+finalE)
                                        .build());
                List<InsertParam.Field> fields = CommonFunction.generateData(dataNum);
                long startTime = System.currentTimeMillis();
                R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder().withCollectionName("collection" + finalE).withFields(fields).build());
                long endTime = System.currentTimeMillis();
                Assert.assertEquals(insert.getStatus().intValue(),0);
                log.info("线程"+finalE+"-用户:Username"+finalE+"导入"+dataNum+"条数据耗时"+(endTime-startTime));
                milvusClient.close();
                return insert;
            };
            Future future = executorService.submit(callable);
            list.add(future);

        }
        for (Future future : list) {
            log.info("运行结果:"+future.get().toString());
        }
        executorService.shutdown();
        }

    @Test(dependsOnMethods = "insertData")
    public void createIndexAndLoadData() throws ExecutionException, InterruptedException {
        int threads=THREAD;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username"+finalE,"Password"+finalE)
                                        .build());
                long startTime = System.currentTimeMillis();
                R<RpcStatus> rpcStatusR =
                        milvusClient.createIndex(
                                CreateIndexParam.newBuilder()
                                        .withCollectionName("collection"+finalE)
                                        .withFieldName(CommonData.defaultVectorField)
                                        .withIndexName(CommonData.defaultIndex)
                                        .withMetricType(MetricType.L2)
                                        .withIndexType(IndexType.HNSW)
                                        .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                                        .withSyncMode(Boolean.TRUE)
                                        .withSyncWaitingTimeout(30L)
                                        .withSyncWaitingInterval(500L)
                                        .build());
                long endTime = System.currentTimeMillis();
                Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
                log.info("线程"+finalE+"-用户:Username"+finalE+"创建索引"+dataNum+"条数据耗时"+(endTime-startTime)+"ms");
                long startTimeLoad = System.currentTimeMillis();
                R<RpcStatus> rpcStatusLoad = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("collection" + finalE)
                        .withSyncLoad(true)
                        .withSyncLoadWaitingInterval(500L)
                        .withSyncLoadWaitingTimeout(300L).build());
                long endTimeLoad = System.currentTimeMillis();
                log.info("线程"+finalE+"-用户:Username"+finalE+"Load"+dataNum+"条数据耗时"+(endTimeLoad-startTimeLoad)+"ms");
                Assert.assertEquals(rpcStatusLoad.getStatus().intValue(), 0);
                milvusClient.close();
                return rpcStatusR;
            };
            Future future = executorService.submit(callable);
            list.add(future);

        }
        for (Future future : list) {
            log.info("运行结果:"+future.get().toString());
        }
        executorService.shutdown();
    }


    @Test(dependsOnMethods = "createIndexAndLoadData")
    public void searchData() throws ExecutionException, InterruptedException {
        int threads=THREAD;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username"+finalE,"Password"+finalE)
                                        .build());
                List<Integer> result=new ArrayList<>();
                int vectorNq=2*finalE>=threads?100*nq:nq;
                for(int i = 0; i < searchNum; i++) {
                Integer SEARCH_K = TopK; // TopK
                String SEARCH_PARAM = "{\"nprobe\":"+nprobe+"}";
                List<String> search_output_fields = Arrays.asList("book_id");

                List<List<Float>> search_vectors = CommonFunction.generateFloatVectors(vectorNq,128);
                SearchParam searchParam =
                        SearchParam.newBuilder()
                                .withCollectionName("collection"+finalE)
                                .withMetricType(MetricType.L2)
                                .withOutFields(search_output_fields)
                                .withTopK(SEARCH_K)
                                .withVectors(search_vectors)
                                .withVectorFieldName(CommonData.defaultVectorField)
                                .withParams(SEARCH_PARAM)
                                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                                .build();
                long startTime = System.currentTimeMillis();
                R<SearchResults> searchResultsR = milvusClient.search(searchParam);
                long endTime = System.currentTimeMillis();
                Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
                SearchResultsWrapper searchResultsWrapper =
                        new SearchResultsWrapper(searchResultsR.getData().getResults());
                Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), TopK);
                log.info("线程"+finalE+"-用户:Username"+finalE+""+"第"+i+"次查询(nq:"+vectorNq+",TopK:"+TopK+")耗时"+(endTime-startTime)+"ms");
                result.add((int) (endTime-startTime));
                }
                milvusClient.close();
                return "线程"+finalE+"测试(nq:"+vectorNq+",TopK:"+TopK+")结果集:"+result;
            };
            Future future = executorService.submit(callable);
            list.add(future);
        }
        for (Future future : list) {
            log.info("运行结果:"+future.get().toString());
        }
        executorService.shutdown();
    }


}
