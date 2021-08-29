package io.milvus.client;

import io.grpc.StatusRuntimeException;
import io.milvus.grpc.BoolResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.HasCollectionRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.R;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    @Override
    public R<Boolean> hasCollection(String collectionName) {
        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .build();

        BoolResponse response;
        try {
            response = blockingStub().hasCollection(hasCollectionRequest);
        } catch (StatusRuntimeException e) {
            logger.error("[milvus] hasCollection:{} request error: {}", collectionName, e.getMessage());
            return R.failed(String.format("rpc request failed:%s", e.getMessage()));
        }
        Boolean aBoolean = Optional.ofNullable(response)
                .map(BoolResponse::getValue)
                .orElse(false);

        return R.success(aBoolean);


    }
}
