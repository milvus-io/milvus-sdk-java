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

package io.milvus.v2.utils;

import io.grpc.StatusRuntimeException;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcUtilsTest {

    @Test
    void testRetryAsyncSuccess() throws Exception {
        RpcUtils rpcUtils = new RpcUtils();

        String result = rpcUtils.retryAsync(() -> CompletableFuture.completedFuture("ok"))
                .get(1, TimeUnit.SECONDS);

        Assertions.assertEquals("ok", result);
    }

    @Test
    void testRetryAsyncRetriesUnavailable() throws Exception {
        RpcUtils rpcUtils = new RpcUtils();
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(3)
                .initialBackOffMs(1)
                .maxBackOffMs(1)
                .backOffMultiplier(1)
                .build());
        AtomicInteger callCount = new AtomicInteger();

        String result = rpcUtils.retryAsync(() -> {
            if (callCount.incrementAndGet() < 3) {
                CompletableFuture<String> failed = new CompletableFuture<>();
                failed.completeExceptionally(new StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE.withDescription("server unavailable")));
                return failed;
            }
            return CompletableFuture.completedFuture("ok");
        }).get(1, TimeUnit.SECONDS);

        Assertions.assertEquals("ok", result);
        Assertions.assertEquals(3, callCount.get());
    }

    @Test
    void testRetryAsyncZeroBackoffDoesNotLoseRetryGeneration() {
        RpcUtils rpcUtils = new RpcUtils();
        int maxRetryTimes = 20;
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(maxRetryTimes)
                .initialBackOffMs(0)
                .maxBackOffMs(0)
                .backOffMultiplier(1)
                .build());

        for (int round = 0; round < 100; round++) {
            AtomicInteger callCount = new AtomicInteger();
            ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                    () -> rpcUtils.retryAsync(() -> {
                        callCount.incrementAndGet();
                        CompletableFuture<String> failed = new CompletableFuture<>();
                        failed.completeExceptionally(new StatusRuntimeException(
                                io.grpc.Status.UNAVAILABLE.withDescription("server unavailable")));
                        return failed;
                    }).get(2, TimeUnit.SECONDS));

            Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
            Assertions.assertEquals(ErrorCode.TIMEOUT,
                    ((MilvusClientException) exception.getCause()).getErrorCode());
            Assertions.assertEquals(maxRetryTimes, callCount.get());
        }
    }

    @Test
    void testRetryAsyncDoesNotRetryInvalidArgument() {
        RpcUtils rpcUtils = new RpcUtils();
        AtomicInteger callCount = new AtomicInteger();
        StatusRuntimeException original = new StatusRuntimeException(
                io.grpc.Status.INVALID_ARGUMENT.withDescription("invalid"));

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> rpcUtils.retryAsync(() -> {
                    callCount.incrementAndGet();
                    CompletableFuture<String> failed = new CompletableFuture<>();
                    failed.completeExceptionally(original);
                    return failed;
                }).get(1, TimeUnit.SECONDS));

        Assertions.assertEquals(1, callCount.get());
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        MilvusClientException clientException = (MilvusClientException) exception.getCause();
        Assertions.assertEquals(ErrorCode.RPC_ERROR, clientException.getErrorCode());
        Assertions.assertSame(original, clientException.getCause());
    }

    @Test
    void testRetryDoesNotRetryInvalidArgumentPreservesCause() {
        RpcUtils rpcUtils = new RpcUtils();
        AtomicInteger callCount = new AtomicInteger();
        StatusRuntimeException original = new StatusRuntimeException(
                io.grpc.Status.INVALID_ARGUMENT.withDescription("invalid"));

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> rpcUtils.retry(() -> {
                    callCount.incrementAndGet();
                    throw original;
                }));

        Assertions.assertEquals(1, callCount.get());
        Assertions.assertEquals(ErrorCode.RPC_ERROR, exception.getErrorCode());
        Assertions.assertSame(original, exception.getCause());
    }

    @Test
    void testRetryAsyncPreservesClientException() {
        RpcUtils rpcUtils = new RpcUtils();
        MilvusClientException original = new MilvusClientException(ErrorCode.INVALID_PARAMS, "invalid");

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> rpcUtils.retryAsync(() -> {
                    CompletableFuture<String> failed = new CompletableFuture<>();
                    failed.completeExceptionally(original);
                    return failed;
                }).get(1, TimeUnit.SECONDS));

        Assertions.assertSame(original, exception.getCause());
    }

    @Test
    void testRetryAsyncCancellationCancelsActiveAttempt() {
        RpcUtils rpcUtils = new RpcUtils();
        CompletableFuture<String> activeAttempt = new CompletableFuture<>();

        CompletableFuture<String> result = rpcUtils.retryAsync(() -> activeAttempt);
        Assertions.assertTrue(result.cancel(true));

        Assertions.assertTrue(activeAttempt.isCancelled());
    }

    @Test
    void testRetryAsyncNoRetryMapsRpcFailure() {
        RpcUtils rpcUtils = new RpcUtils();
        rpcUtils.retryConfig(RetryConfig.builder().maxRetryTimes(1).build());
        AtomicInteger callCount = new AtomicInteger();

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> rpcUtils.retryAsync(() -> {
                    callCount.incrementAndGet();
                    CompletableFuture<String> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new StatusRuntimeException(io.grpc.Status.UNAVAILABLE));
                    return failed;
                }).get(1, TimeUnit.SECONDS));

        Assertions.assertEquals(1, callCount.get());
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.RPC_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
    }

    @Test
    void testEarlyExitWhenPredictedBackoffExceedsMaxRetryTimeoutMs() {
        RpcUtils rpcUtils = new RpcUtils();
        long maxRetryTimeoutMs = 5000;
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(10)
                .maxRetryTimeoutMs(maxRetryTimeoutMs)
                .initialBackOffMs(10)
                .maxBackOffMs(3000)
                .backOffMultiplier(3)
                .build());

        long start = System.currentTimeMillis();

        MilvusClientException thrown = Assertions.assertThrows(MilvusClientException.class, () -> {
            rpcUtils.retry(() -> {
                throw new StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE.withDescription("server unavailable"));
            });
        });

        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertEquals(ErrorCode.TIMEOUT, thrown.getErrorCode(),
                "Should fail with TIMEOUT error code");
        // Backoff sequence (initial=10ms, multiplier=3, max=3000ms):
        //   k=1 @~0ms    → sleep 10ms
        //   k=2 @~10ms   → sleep 30ms
        //   k=3 @~40ms   → sleep 90ms
        //   k=4 @~130ms  → sleep 270ms
        //   k=5 @~400ms  → sleep 810ms
        //   k=6 @~1210ms → sleep 2430ms
        //   k=7 @~3640ms → next backoff 3000ms, 3640+3000=6640 > 5000ms → TIMEOUT
        Assertions.assertTrue(elapsed <= 4000,
                "Retry should respect maxRetryTimeoutMs(5000ms), but took " + elapsed + "ms");
    }

    @Test
    void testMaxRetryTimes() {
        RpcUtils rpcUtils = new RpcUtils();
        int maxRetryTimes = 3;
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(maxRetryTimes)
                .maxRetryTimeoutMs(60000) // large timeout so retry times is the limiting factor
                .initialBackOffMs(10)
                .maxBackOffMs(100)
                .backOffMultiplier(2)
                .build());

        AtomicInteger callCount = new AtomicInteger(0);

        MilvusClientException thrown = Assertions.assertThrows(MilvusClientException.class, () -> {
            rpcUtils.retry(() -> {
                callCount.incrementAndGet();
                throw new StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE.withDescription("server unavailable"));
            });
        });

        Assertions.assertEquals(ErrorCode.TIMEOUT, thrown.getErrorCode(),
                "Should fail with TIMEOUT error code");
        Assertions.assertEquals(maxRetryTimes, callCount.get(),
                "Should have retried exactly maxRetryTimes(" + maxRetryTimes + ") times, but got " + callCount.get());
    }

    @Test
    void testTimeoutAfterSlowCallExceedsMaxRetryTimeoutMs() {
        RpcUtils rpcUtils = new RpcUtils();
        long maxRetryTimeoutMs = 2000;
        rpcUtils.retryConfig(RetryConfig.builder()
                .maxRetryTimes(10)
                .maxRetryTimeoutMs(maxRetryTimeoutMs)
                .initialBackOffMs(50)
                .maxBackOffMs(500)
                .backOffMultiplier(2)
                .build());

        AtomicInteger callCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        MilvusClientException thrown = Assertions.assertThrows(MilvusClientException.class, () -> {
            rpcUtils.retry(() -> {
                callCount.incrementAndGet();
                // Simulate a slow RPC call that takes 500ms each time
                Thread.sleep(500);
                throw new StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE.withDescription("server unavailable"));
            });
        });

        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertEquals(ErrorCode.TIMEOUT, thrown.getErrorCode(),
                "Should fail with TIMEOUT error code when slow calls accumulate beyond maxRetryTimeoutMs");
        // Timeline (slow call sleep=500ms, backoff: initial=50ms, multiplier=2, max=500ms):
        //   k=1 @~0ms     → sleep 500ms, call ends @500ms
        //                    backoff 50ms, total ~550ms < 2000ms → continue
        //   k=2 @~550ms   → sleep 500ms, call ends @~1050ms
        //                    backoff 100ms, total ~1150ms < 2000ms → continue
        //   k=3 @~1150ms   → sleep 500ms, call ends @~1650ms
        //                    backoff 200ms, total ~1850ms < 2000ms → continue
        //   k=4 @~1850ms  → sleep 500ms, call ends @~2350ms
        //                    elapsed(2350ms) >= maxRetryTimeoutMs(2000ms)
        Assertions.assertEquals(4, callCount.get(), "Should have 4 times, but got " + callCount.get());
        Assertions.assertTrue(elapsed > maxRetryTimeoutMs,
                "Elapsed time should greater than maxRetryTimeoutMs, but was " + elapsed + "ms");
        Assertions.assertTrue(elapsed < maxRetryTimeoutMs + 1000,
                "Should not exceed maxRetryTimeoutMs by too much, elapsed was " + elapsed + "ms");
    }
}
