package io.milvus.v2.service.partition;

import io.milvus.grpc.CreatePartitionRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;

import java.util.List;

public class PartitionService extends BaseService {
    public R<RpcStatus> createPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreatePartitionReq request) {
        String title = String.format("Create partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        CreatePartitionRequest createPartitionRequest = io.milvus.grpc.CreatePartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        Status status = blockingStub.createPartition(createPartitionRequest);
        rpcUtils.handleResponse(title, status);

        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> dropPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropPartitionReq request) {
        String title = String.format("Drop partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        io.milvus.grpc.DropPartitionRequest dropPartitionRequest = io.milvus.grpc.DropPartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        Status status = blockingStub.dropPartition(dropPartitionRequest);
        rpcUtils.handleResponse(title, status);

        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<Boolean> hasPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HasPartitionReq request) {
        String title = String.format("Has partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        io.milvus.grpc.HasPartitionRequest hasPartitionRequest = io.milvus.grpc.HasPartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        io.milvus.grpc.BoolResponse boolResponse = blockingStub.hasPartition(hasPartitionRequest);
        rpcUtils.handleResponse(title, boolResponse.getStatus());

        return R.success(boolResponse.getValue());
    }

    public R<List<String>> listPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListPartitionsReq request) {
        String title = String.format("List partitions in collection %s", request.getCollectionName());

        io.milvus.grpc.ShowPartitionsRequest showPartitionsRequest = io.milvus.grpc.ShowPartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName()).build();

        io.milvus.grpc.ShowPartitionsResponse showPartitionsResponse = blockingStub.showPartitions(showPartitionsRequest);
        rpcUtils.handleResponse(title, showPartitionsResponse.getStatus());

        return R.success(showPartitionsResponse.getPartitionNamesList());
    }

    public R<RpcStatus> loadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadPartitionsReq request) {
        String title = String.format("Load partitions %s in collection %s", request.getPartitionNames(), request.getCollectionName());

        io.milvus.grpc.LoadPartitionsRequest loadPartitionsRequest = io.milvus.grpc.LoadPartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames()).build();
        Status status = blockingStub.loadPartitions(loadPartitionsRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> releasePartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ReleasePartitionsReq request) {
        String title = String.format("Release partitions %s in collection %s", request.getPartitionNames(), request.getCollectionName());

        io.milvus.grpc.ReleasePartitionsRequest releasePartitionsRequest = io.milvus.grpc.ReleasePartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames()).build();
        Status status = blockingStub.releasePartitions(releasePartitionsRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }
}
