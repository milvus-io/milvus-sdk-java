package io.milvus.v2.service.vector;

import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.CollectionService;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorService extends BaseService {
    Logger logger = LoggerFactory.getLogger(VectorService.class);
    public CollectionService collectionService = new CollectionService();
    public IndexService indexService = new IndexService();

    public R<RpcStatus> insert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, InsertReq request){
        String title = String.format("InsertRequest collectionName:%s", request.getCollectionName());
        checkCollectionExist(blockingStub, request.getCollectionName());
        DescribeCollectionRequest describeCollectionRequest = DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName()).build();
        DescribeCollectionResponse descResp = blockingStub.describeCollection(describeCollectionRequest);

        MutationResult response = blockingStub.insert(dataUtils.convertGrpcInsertRequest(request, new DescCollResponseWrapper(descResp)));
        rpcUtils.handleResponse(title, response.getStatus());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> upsert(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, UpsertReq request) {
        String title = String.format("UpsertRequest collectionName:%s", request.getCollectionName());

        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());

        DescribeCollectionRequest describeCollectionRequest = DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName()).build();
        DescribeCollectionResponse descResp = milvusServiceBlockingStub.describeCollection(describeCollectionRequest);

        MutationResult response = milvusServiceBlockingStub.upsert(dataUtils.convertGrpcUpsertRequest(request, new DescCollResponseWrapper(descResp)));
        rpcUtils.handleResponse(title, response.getStatus());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<QueryResp> query(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, QueryReq request) {
        String title = String.format("QueryRequest collectionName:%s", request.getCollectionName());
        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());
        R<DescribeCollectionResp> descR = collectionService.describeCollection(milvusServiceBlockingStub, DescribeCollectionReq.builder().collectionName(request.getCollectionName()).build());
        if(request.getOutputFields() == null){
            request.setOutputFields(descR.getData().getFieldNames());
        }
        QueryResults response = milvusServiceBlockingStub.query(vectorUtils.ConvertToGrpcQueryRequest(request));
        rpcUtils.handleResponse(title, response.getStatus());

        QueryResp res = QueryResp.builder()
                .queryResults(convertUtils.getEntities(response))
                .build();
        return R.success(res);

    }

    public R<SearchResp> search(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, SearchReq request) {
        String title = String.format("SearchRequest collectionName:%s", request.getCollectionName());

        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());
        R<DescribeCollectionResp> descR = collectionService.describeCollection(milvusServiceBlockingStub, DescribeCollectionReq.builder().collectionName(request.getCollectionName()).build());
        if (request.getVectorFieldName() == null) {
            request.setVectorFieldName(descR.getData().getVectorFieldName().get(0));
        }
        if(request.getOutFields() == null){
            request.setOutFields(descR.getData().getFieldNames());
        }
        DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                .collectionName(request.getCollectionName())
                .fieldName(request.getVectorFieldName())
                .build();
        R<DescribeIndexResp> respR = indexService.describeIndex(milvusServiceBlockingStub, describeIndexReq);

        SearchRequest searchRequest = vectorUtils.ConvertToGrpcSearchRequest(respR.getData().getMetricType(), request);

        SearchResults response = milvusServiceBlockingStub.search(searchRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        SearchResp searchResp = SearchResp.builder()
                .searchResults(convertUtils.getEntities(response))
                .build();
        return R.success(searchResp);
    }

    public R<RpcStatus> delete(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DeleteReq request) {
        String title = String.format("DeleteRequest collectionName:%s", request.getCollectionName());
        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());
        R<DescribeCollectionResp> respR = collectionService.describeCollection(milvusServiceBlockingStub, DescribeCollectionReq.builder().collectionName(request.getCollectionName()).build());
        if(request.getExpr() == null){
            request.setExpr(vectorUtils.getExprById(respR.getData().getPrimaryFieldName(), request.getIds()));
        }
        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName())
                .setExpr(request.getExpr())
                .build();
        MutationResult response = milvusServiceBlockingStub.delete(deleteRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<GetResp> get(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, GetReq request) {
        String title = String.format("GetRequest collectionName:%s", request.getCollectionName());
        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());
        DescribeCollectionReq describeCollectionReq = DescribeCollectionReq.builder()
                .collectionName(request.getCollectionName())
                .build();
        R<DescribeCollectionResp> resp = collectionService.describeCollection(milvusServiceBlockingStub, describeCollectionReq);

        String expr = vectorUtils.getExprById(resp.getData().getPrimaryFieldName(), request.getIds());
        QueryReq queryReq = QueryReq.builder()
                .collectionName(request.getCollectionName())
                .expr(expr)
                .build();
        R<QueryResp> queryResp = query(milvusServiceBlockingStub, queryReq);

        GetResp getResp = GetResp.builder()
                .getResults(queryResp.getData().getQueryResults())
                .build();
        return R.success(getResp);
    }
}
