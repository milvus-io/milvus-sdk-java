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

package io.milvus.unit.v2.utils;

import io.grpc.StatusRuntimeException;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.RpcUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tag("unit")
public class RpcAsyncSchedulerTest {

    @Test
    void testAsyncRetryExecutorRemovesCancelledTasks() throws Exception {
        Field field = RpcUtils.class.getDeclaredField("asyncRetryExecutor");
        field.setAccessible(true);
        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) field.get(new RpcUtils());

        Assertions.assertTrue(executor.getRemoveOnCancelPolicy());
    }

    @Test
    void testShutdownCancelsPendingScheduledRetry() throws Exception {
        RpcUtils rpcUtils = new RpcUtils();
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(5)
                .initialBackOffMs(60000)
                .maxBackOffMs(60000)
                .backOffMultiplier(1)
                .build());
        AtomicInteger callCount = new AtomicInteger();
        CompletableFuture<String> result = rpcUtils.retryAsync(() -> {
            callCount.incrementAndGet();
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(new StatusRuntimeException(
                    io.grpc.Status.UNAVAILABLE.withDescription("server unavailable")));
            return failed;
        });

        rpcUtils.shutdown();

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> result.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        Assertions.assertEquals(1, callCount.get(),
                "shutdown should cancel the pending scheduled retry");
    }

    @Test
    void testShutdownFailsInFlightAttempt() throws Exception {
        RpcUtils rpcUtils = new RpcUtils();
        CompletableFuture<String> inFlight = new CompletableFuture<>();
        CompletableFuture<String> result = rpcUtils.retryAsync(() -> inFlight);

        rpcUtils.shutdown();

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> result.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        MilvusClientException clientException = (MilvusClientException) exception.getCause();
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR, clientException.getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", clientException.getMessage(),
                "the intended exception must not be lost to a cancellation-wrapped error");
        Assertions.assertTrue(inFlight.isCancelled());
    }

    @Test
    void testRetryAsyncRecreatesSchedulerAfterShutdown() throws Exception {
        RpcUtils rpcUtils = new RpcUtils();
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(3)
                .initialBackOffMs(10)
                .maxBackOffMs(10)
                .backOffMultiplier(1)
                .build());
        rpcUtils.shutdown();
        AtomicInteger callCount = new AtomicInteger();

        String result = rpcUtils.retryAsync(() -> {
            if (callCount.incrementAndGet() < 2) {
                CompletableFuture<String> failed = new CompletableFuture<>();
                failed.completeExceptionally(new StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE.withDescription("server unavailable")));
                return failed;
            }
            return CompletableFuture.completedFuture("ok");
        }).get(1, TimeUnit.SECONDS);

        Assertions.assertEquals("ok", result);
        Assertions.assertEquals(2, callCount.get(),
                "a retryable failure after shutdown should schedule a retry, forcing scheduler recreation");
    }
}
