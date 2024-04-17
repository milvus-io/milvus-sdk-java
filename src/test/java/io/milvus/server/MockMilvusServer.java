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

import io.grpc.ServerBuilder;
import io.milvus.grpc.MilvusServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockMilvusServer extends MockServer {
    private static final Logger logger = LoggerFactory.getLogger(MockMilvusServer.class.getName());

    private final MilvusServiceGrpc.MilvusServiceImplBase serviceImpl;

    public MockMilvusServer(int port, MilvusServiceGrpc.MilvusServiceImplBase impl) {
        super(port);
        serviceImpl = impl;
    }

    @Override
    public void start() {
        try {
            rpcServer = ServerBuilder.forPort(serverPort)
                    .addService(serviceImpl)
                    .build()
                    .start();
        } catch (Exception e) {
            logger.error("Failed to start MockServer on port: " + serverPort);
            return;
        }

        logger.info("MockServer started on port: " + serverPort);
        Runtime.getRuntime().addShutdownHook(new Thread(MockMilvusServer.this::stop));
    }
}
