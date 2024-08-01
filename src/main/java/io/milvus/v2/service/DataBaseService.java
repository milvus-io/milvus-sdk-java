package io.milvus.v2.service;

import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;

import java.util.List;

public class DataBaseService extends BaseService {
    public List<String> listDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "ListDatabase";
        ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder().build();
        ListDatabasesResponse listDatabasesResponse = blockingStub.listDatabases(listDatabasesRequest);
        rpcUtils.handleResponse(title, listDatabasesResponse.getStatus());
        return listDatabasesResponse.getDbNamesList();
    }
}
