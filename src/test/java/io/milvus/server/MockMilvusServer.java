/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.milvus.grpc.MilvusServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
