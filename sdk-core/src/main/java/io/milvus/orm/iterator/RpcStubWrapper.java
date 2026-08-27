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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around a Milvus gRPC blocking stub that carries the endpoint and database name of the
 * client it was created from, and applies the per-call RPC deadline used by the iterators.
 *
 * <p>The wrapped stub is reused across iterator calls; each call to {@link #get()} returns a stub
 * with the deadline reset so the deadline applies to that single call.
 */
public class RpcStubWrapper {
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;

    // rpcTimeoutMs of MilvusServiceBlockingStub.withDeadlineAfter() is "end of using time", not "timeout of per call",
    // we have to reset this value for each time QueryIterator calls the query() interface.
    // the rpcDeadlineMs value is passed from MilvusClient
    private final long rpcDeadlineMs;
    private final String endpoint;
    private final String databaseName;

    /**
     * Creates a stub wrapper for the given blocking stub.
     *
     * @param blockingStub the Milvus gRPC blocking stub, must not be {@code null}
     * @param rpcDeadlineMs the per-call RPC deadline in milliseconds, or {@code 0} for no deadline
     * @param endpoint the server endpoint, must not be empty
     * @param databaseName the database name; empty is normalized to {@code "default"}
     * @throws IllegalArgumentException if {@code endpoint} is empty
     */
    public RpcStubWrapper(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                          long rpcDeadlineMs,
                          String endpoint,
                          String databaseName) {
        this.blockingStub = Objects.requireNonNull(blockingStub, "blockingStub cannot be null");
        this.rpcDeadlineMs = rpcDeadlineMs;
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Cache endpoint cannot be empty");
        }
        this.endpoint = endpoint;
        this.databaseName = databaseName == null || databaseName.isEmpty() ? "default" : databaseName;
    }

    /**
     * Returns the blocking stub, optionally with the configured RPC deadline applied.
     *
     * <p>When {@code rpcDeadlineMs} is greater than zero, a new stub with a fresh
     * {@code withDeadlineAfter} is returned for each call so that the deadline applies to a single
     * call instead of the lifetime of the stub.
     *
     * @return the blocking stub
     */
    public MilvusServiceGrpc.MilvusServiceBlockingStub get() {
        if (rpcDeadlineMs > 0) {
            return blockingStub.withDeadlineAfter(rpcDeadlineMs, TimeUnit.MILLISECONDS);
        } else {
            return blockingStub;
        }
    }

    /**
     * Returns the server endpoint.
     *
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }
}
