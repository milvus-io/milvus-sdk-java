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

package io.milvus.client;

import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.ConnectParam;

import lombok.NonNull;
import java.util.concurrent.TimeUnit;

public class MilvusServiceClient extends AbstractMilvusGrpcClient {

    private final ManagedChannel channel;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final MilvusServiceGrpc.MilvusServiceFutureStub futureStub;

    public MilvusServiceClient(@NonNull ConnectParam connectParam) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), connectParam.getAuthorization());

        channel = ManagedChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .keepAliveTime(connectParam.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(connectParam.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
                .idleTimeout(connectParam.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .build();
        blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
        futureStub = MilvusServiceGrpc.newFutureStub(channel);
    }

    @Override
    protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
        return this.blockingStub;
    }

    @Override
    protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
        return this.futureStub;
    }

    @Override
    protected boolean clientIsReady() {
        ConnectivityState state = channel.getState(false);
        return state != ConnectivityState.SHUTDOWN;
    }

    @Override
    public void close(long maxWaitSeconds) throws InterruptedException {
        channel.shutdownNow();
        channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
    }

    private static class TimeoutInterceptor implements ClientInterceptor {
        private final long timeoutMillis;

        TimeoutInterceptor(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return next.newCall(method, callOptions.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
        final long timeoutMillis = timeoutUnit.toMillis(timeout);
        final TimeoutInterceptor timeoutInterceptor = new TimeoutInterceptor(timeoutMillis);
        final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStubTimeout =
                this.blockingStub.withInterceptors(timeoutInterceptor);
        final MilvusServiceGrpc.MilvusServiceFutureStub futureStubTimeout =
                this.futureStub.withInterceptors(timeoutInterceptor);

        return new AbstractMilvusGrpcClient() {
            @Override
            protected boolean clientIsReady() {
                return MilvusServiceClient.this.clientIsReady();
            }

            @Override
            protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
                return blockingStubTimeout;
            }

            @Override
            protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
                return futureStubTimeout;
            }

            @Override
            public void close(long maxWaitSeconds) throws InterruptedException {
                MilvusServiceClient.this.close(maxWaitSeconds);
            }

            @Override
            public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
                return MilvusServiceClient.this.withTimeout(timeout, timeoutUnit);
            }
        };
    }
}

