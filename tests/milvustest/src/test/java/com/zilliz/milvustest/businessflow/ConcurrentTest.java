package com.zilliz.milvustest.businessflow;

import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.role.AddUserToRoleParam;
import io.milvus.param.role.CreateRoleParam;
import io.milvus.param.role.GrantRolePrivilegeParam;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author yongpeng.li
 * @Date 2023/5/5 11:27
 */

public class ConcurrentTest {

    @DataProvider(name = "UserInfo")
    public Object[][] provideUser(){
        String[][] userinfo=new String[20][2];
        for(int i = 0; i < 20; i++) {
         userinfo[i][0]="Username"+i;
         userinfo[i][1]="Password"+i;
        }
        return userinfo;
    }

    @Test(dataProvider = "UserInfo")
    public void registerUserInfo(String username,String password){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost("10.102.9.108")
                                .withPort(19530)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        R<RpcStatus> credential = milvusClient.createCredential(CreateCredentialParam.newBuilder().withUsername(username).withPassword(password).build());
        System.out.println(credential.getStatus());
        System.out.println(credential.getData().toString());

    }

    @Test(description = "Create role")
    public void createRole() {
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost("10.102.9.108")
                                .withPort(19530)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        /*R<RpcStatus> role =
                milvusClient.createRole(
                        CreateRoleParam.newBuilder().withRoleName("newRole").build());
        Assert.assertEquals(role.getStatus().intValue(), 0);
        Assert.assertEquals(role.getData().getMsg(), "Success");*/
        R<RpcStatus> rpcStatusR =
                milvusClient.grantRolePrivilege(
                        GrantRolePrivilegeParam.newBuilder()
                                .withRoleName("newRole")
                                .withObject("Collection")
                                .withObjectName("*")
                                .withPrivilege("GetStatistics")
                                .build());
    }

    @Test(dataProvider = "UserInfo")
    public void addUserToRole(String username,String password){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost("10.102.9.108")
                                .withPort(19530)
                                .withSecure(false)
                                .withAuthorization("root","Milvus")
                                .build());
        R<RpcStatus> rpcStatusR =
                milvusClient.addUserToRole(
                        AddUserToRoleParam.newBuilder()
                                .withUserName(username)
                                .withRoleName("newRole")
                                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    }

    @Test
    public void createCollection() throws ExecutionException, InterruptedException {
        int threads=20;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost("10.102.9.108")
                                        .withPort(19530)
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
                System.out.println("线程"+finalE+":用户Username"+finalE+"创建collection："+collectionName);
                System.out.println(collection.getStatus());
                milvusClient.close();
                return collection;
            };
            Future future = executorService.submit(callable);
            list.add(future);
    }
        for (Future future : list) {
            System.out.println("运行结果:"+future.get().toString());
        }
        executorService.shutdown();

    }

    @Test
    public void showCollection() throws ExecutionException, InterruptedException {
        int threads=10;
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost("10.102.9.108")
                                        .withPort(19530)
                                        .withSecure(false)
                                        .withAuthorization("Username"+finalE,"Password"+finalE)
                                        .build());
                R<ShowCollectionsResponse> showCollectionsResponseR = milvusClient.showCollections(ShowCollectionsParam.newBuilder().withShowType(ShowType.All).build());
                System.out.println("线程"+finalE+":用户Username"+finalE+"show collection："+showCollectionsResponseR.getData());
                milvusClient.close();
                return showCollectionsResponseR;
            };
            Future future = executorService.submit(callable);
            list.add(future);

        }
        for (Future future : list) {
            System.out.println("运行结果:"+future.get().toString());
        }
        executorService.shutdown();
        }
}
