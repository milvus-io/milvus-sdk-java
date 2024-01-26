package io.milvus.v2.service.index;

import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

public class IndexService extends BaseService {
    public R<RpcStatus> createIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, CreateIndexReq request) {
        String title = String.format("CreateIndexRequest collectionName:%s, fieldName:%s",
                request.getCollectionName(), request.getIndexParam().getFieldName());
        CreateIndexRequest createIndexRequest = CreateIndexRequest.newBuilder()
                        .setCollectionName(request.getCollectionName())
                .setIndexName(request.getIndexParam().getIndexName())
                .setFieldName(request.getIndexParam().getFieldName())
                .addExtraParams(KeyValuePair.newBuilder()
                        .setKey("index_type")
                        .setValue(String.valueOf(request.getIndexParam().getIndexType()))
                        .build())
                .addExtraParams(KeyValuePair.newBuilder()
                        .setKey("metric_type")
                        .setValue(String.valueOf(request.getIndexParam().getMetricType()))
                        .build())
                .build();

        Status status = milvusServiceBlockingStub.createIndex(createIndexRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> dropIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DropIndexReq request) {
        String title = String.format("DropIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName())
                .setIndexName(request.getIndexName())
                .build();

        Status status = milvusServiceBlockingStub.dropIndex(dropIndexRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<DescribeIndexResp> describeIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DescribeIndexReq request) {
        String title = String.format("DescribeIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName())
                .setIndexName(request.getIndexName())
                .build();

        DescribeIndexResponse response = milvusServiceBlockingStub.describeIndex(describeIndexRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        return convertUtils.convertToDescribeIndexResp(response);
    }
}
