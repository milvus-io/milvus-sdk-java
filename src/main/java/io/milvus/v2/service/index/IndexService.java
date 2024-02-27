package io.milvus.v2.service.index;

import io.milvus.grpc.*;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.ArrayList;
import java.util.List;

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
                            .setValue(indexParam.getIndexType().getName())
                            .build())
                    .build();
            if(indexParam.getMetricType()!= null){
                // only vector field has a metric type
                createIndexRequest = createIndexRequest.toBuilder()
                        .addExtraParams(KeyValuePair.newBuilder()
                                .setKey("metric_type")
                                .setValue(indexParam.getMetricType().name())
                                .build())
                        .build();
            }
            if (indexParam.getExtraParams() != null) {
                for (String key : indexParam.getExtraParams().keySet()) {
                    createIndexRequest = createIndexRequest.toBuilder()
                            .addExtraParams(KeyValuePair.newBuilder()
                                    .setKey(key)
                                    .setValue(String.valueOf(indexParam.getExtraParams().get(key)))
                                    .build())
                            .build();
                }
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
                .setFieldName(request.getFieldName() == null ? "" : request.getFieldName())
                .setIndexName(request.getIndexName() == null ? "" : request.getIndexName())
                .build();

        Status status = milvusServiceBlockingStub.dropIndex(dropIndexRequest);
        rpcUtils.handleResponse(title, status);
    }

    public DescribeIndexResp describeIndex(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DescribeIndexReq request) {
        String title = String.format("DescribeIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName() == null ? "" : request.getFieldName())
                .setIndexName(request.getIndexName() == null ? "" : request.getIndexName())
                .build();

        DescribeIndexResponse response = milvusServiceBlockingStub.describeIndex(describeIndexRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        return convertUtils.convertToDescribeIndexResp(response);
    }

    public List<String> listIndexes(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListIndexesReq request) {
        String title = String.format("ListIndexesRequest collectionName:%s", request.getCollectionName());
        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();

        DescribeIndexResponse response = blockingStub.describeIndex(describeIndexRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        List<String> indexNames = new ArrayList<>();
        response.getIndexDescriptionsList().forEach(index -> {
            indexNames.add(index.getIndexName());
        });
        return indexNames;
    }
}
