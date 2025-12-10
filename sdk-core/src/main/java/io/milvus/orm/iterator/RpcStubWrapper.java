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

package io.milvus.orm.iterator;

import io.milvus.grpc.MilvusServiceGrpc;

import java.util.concurrent.TimeUnit;

public class RpcStubWrapper {
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;

    // rpcTimeoutMs of MilvusServiceBlockingStub.withDeadlineAfter() is "end of using time", not "timeout of per call",
    // we have to reset this value for each time QueryIterator calls the query() interface.
    // the rpcDeadlineMs value is passed from MilvusClient
    private long rpcDeadlineMs = 0L;

    public RpcStubWrapper(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                          long rpcDeadlineMs) {
        this.blockingStub = blockingStub;
        this.rpcDeadlineMs = rpcDeadlineMs;
    }

    public MilvusServiceGrpc.MilvusServiceBlockingStub get() {
        if (rpcDeadlineMs > 0) {
            return blockingStub.withDeadlineAfter(rpcDeadlineMs, TimeUnit.MILLISECONDS);
        } else {
            return blockingStub;
        }
    }
}
