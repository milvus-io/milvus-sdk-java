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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.Control.GetMetricsRequestParam;
import io.milvus.param.Control.GetPersistentSegmentInfoParam;
import io.milvus.param.Control.GetQuerySegmentInfoParam;
import io.milvus.param.R;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MilvusServiceClient extends AbstractMilvusGrpcClient {

    private final ManagedChannel channel;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final MilvusServiceGrpc.MilvusServiceFutureStub futureStub;

    public MilvusServiceClient(ConnectParam connectParam) {
        channel = ManagedChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .keepAliveTime(connectParam.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(connectParam.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
                .idleTimeout(connectParam.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
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
    protected boolean maybeAvailable() {
        switch (channel.getState(false)) {
            case IDLE:
            case CONNECTING:
            case READY:
                return true;
            default:
                return false;
        }
    }

    @Override
    public void close(long maxWaitSeconds) {

    }

    public static void main(String[] args) {
        ConnectParam build = ConnectParam.Builder.newBuilder()
                .withHost("192.168.182.132")
                .withPort(19530)
                .build();

        MilvusServiceClient milvusServiceClient = new MilvusServiceClient(build);

        //R<Boolean> chuwutest = milvusServiceClient.hasCollection("chuwutest");
        //GetPersistentSegmentInfoParam test = new GetPersistentSegmentInfoParam(GetPersistentSegmentInfoParam.Builder.newBuilder().withCollectionName("hello_milvus"));
        //R<GetPersistentSegmentInfoResponse> zhaoTest = milvusServiceClient.getPersistentSegmentInfo(test);
        
        //GetQuerySegmentInfoParam test = new GetQuerySegmentInfoParam(GetQuerySegmentInfoParam.Builder.newBuilder().withCollectionName("hello_milvus"));
        //R<GetQuerySegmentInfoResponse> zhaoTest = milvusServiceClient.getQuerySegmentInfo(test);

        GetMetricsRequestParam test = new GetMetricsRequestParam(GetMetricsRequestParam.Builder.newBuilder().withCollectionName("{zhao:1}"));
        R<GetMetricsResponse> zhaoTest = milvusServiceClient.getMetrics(test);
        System.out.println(zhaoTest);
    }

}

