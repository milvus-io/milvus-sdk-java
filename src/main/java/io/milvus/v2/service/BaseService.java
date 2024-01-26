package io.milvus.v2.service;

import io.milvus.exception.MilvusException;
import io.milvus.grpc.BoolResponse;
import io.milvus.grpc.HasCollectionRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.R;
import io.milvus.v2.utils.ConvertUtils;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.VectorUtils;
import io.milvus.v2.utils.RpcUtils;

public class BaseService {
    public RpcUtils rpcUtils = new RpcUtils();
    public DataUtils dataUtils = new DataUtils();
    public VectorUtils vectorUtils = new VectorUtils();
    public ConvertUtils convertUtils = new ConvertUtils();

    protected void checkCollectionExist(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String collectionName) {
        HasCollectionRequest request = HasCollectionRequest.newBuilder().setCollectionName(collectionName).build();
        BoolResponse result = blockingStub.hasCollection(request);
        rpcUtils.handleResponse("", result.getStatus());
        if (!result.getValue() == Boolean.TRUE) {
            throw new MilvusException("Collection " + collectionName + " not exist", R.Status.CollectionNotExists.getCode());
        }
    }
}
