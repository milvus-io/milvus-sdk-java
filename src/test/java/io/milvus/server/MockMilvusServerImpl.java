package io.milvus.server;

import io.milvus.client.AbstractMilvusGrpcClient;
import io.milvus.grpc.MilvusServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockMilvusServerImpl extends MilvusServiceGrpc.MilvusServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(MockMilvusServerImpl.class);

    public MockMilvusServerImpl() {
    }

    @Override
    public void createCollection(io.milvus.grpc.CreateCollectionRequest request,
                                 io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("createCollection() call");

        responseObserver.onNext(respCreateCollection);
        responseObserver.onCompleted();
    }

    private io.milvus.grpc.Status respCreateCollection;

    public void setCreateCollectionResponse(io.milvus.grpc.Status status) {
        respCreateCollection = status;
    }

    @Override
    public void hasCollection(io.milvus.grpc.HasCollectionRequest request,
                              io.grpc.stub.StreamObserver<io.milvus.grpc.BoolResponse> responseObserver) {
        logger.info("hasCollection() call");

        responseObserver.onNext(respHasCollection);
        responseObserver.onCompleted();
    }

    private io.milvus.grpc.BoolResponse respHasCollection;

    public void setHasCollectionResponse(io.milvus.grpc.BoolResponse resp) {
        respHasCollection = resp;
    }

    @Override
    public void flush(io.milvus.grpc.FlushRequest request,
                      io.grpc.stub.StreamObserver<io.milvus.grpc.FlushResponse> responseObserver) {
        logger.info("flush() call");

        responseObserver.onNext(respFlush);
        responseObserver.onCompleted();
    }

    private io.milvus.grpc.FlushResponse respFlush;

    public void setFlushResponse(io.milvus.grpc.FlushResponse resp) {
        respFlush = resp;
    }
}
