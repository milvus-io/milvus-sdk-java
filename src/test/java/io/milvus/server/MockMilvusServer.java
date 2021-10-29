package io.milvus.server;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockMilvusServer {
    private static final Logger logger = LoggerFactory.getLogger(MockMilvusServer.class.getName());

    private Server rpcServer;
    private int serverPort;

    private MilvusServiceGrpc.MilvusServiceImplBase serviceImpl;

    public MockMilvusServer(int port, MilvusServiceGrpc.MilvusServiceImplBase impl) {
        serverPort = port;
        serviceImpl = impl;
    }

    public void start() {
        try {
            rpcServer = ServerBuilder.forPort(serverPort)
                    .addService(serviceImpl)
                    .build()
                    .start();
        } catch (Exception e) {
            logger.error("Failed to start server on port: " + serverPort);
            return;
        }

        logger.info("Server started on port: " + serverPort);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                MockMilvusServer.this.stop();
            }
        });
    }

    public void stop() {
        if (rpcServer != null) {
            logger.info("RPC server is shutting down...");
            try {
                rpcServer.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failed to shutdown RPC server");
            }
            rpcServer = null;
            logger.info("Server stopped");
        }
    }
}
