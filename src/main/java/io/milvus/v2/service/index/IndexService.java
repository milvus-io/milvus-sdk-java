package io.milvus.v2.service.index;

import io.milvus.grpc.*;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

public class IndexService extends BaseService {

    public void createIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, CreateIndexReq request) {
        for(IndexParam indexParam : request.getIndexParams()) {
            String title = String.format("CreateIndexRequest collectionName:%s, fieldName:%s",
                    request.getCollectionName(), indexParam.getFieldName());
            CreateIndexRequest createIndexRequest = CreateIndexRequest.newBuilder()
                    .setCollectionName(request.getCollectionName())
                    .setIndexName(indexParam.getIndexName())
                    .setFieldName(indexParam.getFieldName())
                    .addExtraParams(KeyValuePair.newBuilder()
                            .setKey("index_type")
                            .setValue(String.valueOf(indexParam.getIndexType()))
                            .build())
                    .build();
            if(indexParam.getMetricType()!= null){
                // only vector field has a metric type
                createIndexRequest = createIndexRequest.toBuilder()
                        .addExtraParams(KeyValuePair.newBuilder()
                                .setKey("metric_type")
                                .setValue(String.valueOf(indexParam.getMetricType()))
                                .build())
                        .build();
            }

            Status status = milvusServiceBlockingStub.createIndex(createIndexRequest);
            rpcUtils.handleResponse(title, status);
        }
    }

    public void dropIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DropIndexReq request) {
        String title = String.format("DropIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName())
                .setIndexName(request.getIndexName())
                .build();

        Status status = milvusServiceBlockingStub.dropIndex(dropIndexRequest);
        rpcUtils.handleResponse(title, status);
    }

    public DescribeIndexResp describeIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DescribeIndexReq request) {
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
