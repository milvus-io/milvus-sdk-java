package io.milvus.v2.service.utility;

import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;

public class UtilityService extends BaseService {
    public R<RpcStatus> flush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushReq request) {
        String title = String.format("Flush collection %s", request.getCollectionName());
        io.milvus.grpc.FlushRequest flushRequest = io.milvus.grpc.FlushRequest.newBuilder()
                .addCollectionNames(request.getCollectionName())
                .build();
        FlushResponse status = blockingStub.flush(flushRequest);
        rpcUtils.handleResponse(title, status.getStatus());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public void createAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateAliasReq request) {
        String title = String.format("Create alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.CreateAliasRequest createAliasRequest = io.milvus.grpc.CreateAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.createAlias(createAliasRequest);
        rpcUtils.handleResponse(title, status);
    }

    public void dropAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropAliasReq request) {
        String title = String.format("Drop alias %s", request.getAlias());
        io.milvus.grpc.DropAliasRequest dropAliasRequest = io.milvus.grpc.DropAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.dropAlias(dropAliasRequest);
        rpcUtils.handleResponse(title, status);
    }

    public void alterAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterAliasReq request) {
        String title = String.format("Alter alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.AlterAliasRequest alterAliasRequest = io.milvus.grpc.AlterAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.alterAlias(alterAliasRequest);
        rpcUtils.handleResponse(title, status);
    }

    public DescribeAliasResp describeAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeAliasReq request) {
        String title = String.format("Describe alias %s", request.getAlias());
        io.milvus.grpc.DescribeAliasRequest describeAliasRequest = io.milvus.grpc.DescribeAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.DescribeAliasResponse response = blockingStub.describeAlias(describeAliasRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return DescribeAliasResp.builder()
                .collectionName(response.getCollection())
                .alias(response.getAlias())
                .build();
    }

    public ListAliasResp listAliases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListAliasesReq request) {
        String title = "List aliases";
        io.milvus.grpc.ListAliasesRequest listAliasesRequest = io.milvus.grpc.ListAliasesRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        io.milvus.grpc.ListAliasesResponse response = blockingStub.listAliases(listAliasesRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return ListAliasResp.builder()
                .collectionName(response.getCollectionName())
                .alias(response.getAliasesList())
                .build();
    }
}
