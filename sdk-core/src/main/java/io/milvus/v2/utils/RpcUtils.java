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
import io.milvus.grpc.Status;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class RpcUtils {

    protected static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);
    private static final String GLOBAL_ROUTING_ERROR = "STREAMING_CODE_REPLICATE_VIOLATION";
    // Owned per client so the scheduler can be shut down on client close and tuned per client.
    // The single daemon thread is only spawned lazily on the first scheduled retry. Lazily
    // recreated on demand so a reconnect after close() (e.g. useDatabase) keeps async working.
    private volatile ScheduledThreadPoolExecutor asyncRetryExecutor = createAsyncRetryExecutor();
    private volatile RetryConfig retryConfig = RetryConfig.builder().build();
    private volatile Runnable globalRefreshTrigger;

    private static ScheduledThreadPoolExecutor createAsyncRetryExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "milvus-async-retry");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * Shuts down the async retry scheduler. Pending retries are cancelled; a later
     * {@link #retryAsync} call lazily recreates the scheduler and keeps working.
     */
    public void shutdown() {
        asyncRetryExecutor.shutdownNow();
    }

    private ScheduledThreadPoolExecutor asyncRetryExecutor() {
        ScheduledThreadPoolExecutor executor = asyncRetryExecutor;
        if (executor == null || executor.isShutdown()) {
            synchronized (this) {
                executor = asyncRetryExecutor;
                if (executor == null || executor.isShutdown()) {
                    executor = createAsyncRetryExecutor();
                    asyncRetryExecutor = executor;
                }
            }
        }
        return executor;
    }

    public void retryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
    }

    public void setGlobalRefreshTrigger(Runnable trigger) {
        this.globalRefreshTrigger = trigger;
    }

    private void handleGlobalConnectionError(StatusRuntimeException e) {
        if (globalRefreshTrigger == null) {
            return;
        }
        if (e.getStatus().getCode() == io.grpc.Status.UNAVAILABLE.getCode()) {
            logger.info("Connection unavailable, triggering global topology refresh: {}", e.getMessage());
            try {
                globalRefreshTrigger.run();
            } catch (Exception ex) {
                logger.warn("Failed to trigger global topology refresh: {}", ex.getMessage());
            }
        }
    }

    private boolean handleGlobalRoutingError(Exception e) {
        if (globalRefreshTrigger == null) {
            return false;
        }
        String message = e.getMessage();
        if (message != null && message.contains(GLOBAL_ROUTING_ERROR)) {
            logger.info("Detected {}, triggering global topology refresh", GLOBAL_ROUTING_ERROR);
            try {
                globalRefreshTrigger.run();
            } catch (Exception ex) {
                logger.warn("Failed to trigger global topology refresh: {}", ex.getMessage());
            }
            return true;
        }
        return false;
    }

    public void handleResponse(String requestInfo, Status status) {
        // the server made a change for error code:
        // for 2.2.x, error code is status.getErrorCode()
        // for 2.3.x, error code is status.getCode(), and the status.getErrorCode()
        // is also assigned according to status.getCode()
        //
        // For error cases:
        // if we use 2.3.4 sdk to interact with 2.3.x server, getCode() is non-zero, getErrorCode() is non-zero
        // if we use 2.3.4 sdk to interact with 2.2.x server, getCode() is zero, getErrorCode() is non-zero
        // if we use <=2.3.3 sdk to interact with 2.2.x/2.3.x server, getCode() is not available, getErrorCode() is non-zero

        if (status.getCode() != 0 || !status.getErrorCode().equals(io.milvus.grpc.ErrorCode.Success)) {

            // 2.3.4 sdk to interact with 2.2.x server, the getCode() is zero, here we reset its value to getErrorCode()
            int code = status.getCode();
            if (code == 0) {
                code = status.getErrorCode().getNumber();
            }
            logger.error("{} failed, error code: {}, reason: {}", requestInfo, ErrorCode.SERVER_ERROR.getCode(),
                    status.getReason());
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, status.getReason(),
                    code, status.getErrorCode().getNumber());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("{} successfully!", requestInfo);
        }
    }

    public <T> T retry(Callable<T> callable) {
        int maxRetryTimes = retryConfig.getMaxRetryTimes();
        // no retry, direct call the method
        if (maxRetryTimes <= 1) {
            try {
                return callable.call();
            } catch (StatusRuntimeException e) {
                throw new MilvusClientException(ErrorCode.RPC_ERROR, e); // rpc error
            } catch (MilvusClientException e) {
                throw e; // server error or client error
            } catch (Exception e) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e); // others error treated as client error
            }
        }

        // method to check timeout
        long begin = System.currentTimeMillis();
        long maxRetryTimeoutMs = retryConfig.getMaxRetryTimeoutMs();
        Function<Long, Boolean> timeoutChecker = (timePoint) -> {
            long cost = (timePoint - begin);
            if (maxRetryTimeoutMs > 0 && cost >= maxRetryTimeoutMs) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        };

        // retry within timeout
        long retryIntervalMs = retryConfig.getInitialBackOffMs();
        for (int k = 1; k <= maxRetryTimes; k++) {
            try {
                return callable.call();
            } catch (StatusRuntimeException e) {
                io.grpc.Status.Code code = e.getStatus().getCode();
                if (code == io.grpc.Status.DEADLINE_EXCEEDED.getCode()
                        || code == io.grpc.Status.PERMISSION_DENIED.getCode()
                        || code == io.grpc.Status.UNAUTHENTICATED.getCode()
                        || code == io.grpc.Status.INVALID_ARGUMENT.getCode()
                        || code == io.grpc.Status.ALREADY_EXISTS.getCode()
                        || code == io.grpc.Status.RESOURCE_EXHAUSTED.getCode()
                        || code == io.grpc.Status.UNIMPLEMENTED.getCode()) {
                    String msg = String.format("Encounter rpc error that cannot be retried, reason: %s", e);
                    logger.error(msg);
                    throw new MilvusClientException(ErrorCode.RPC_ERROR, e); // throw rpc error
                }

                // trigger topology refresh if connection is unavailable, and continue to retry
                handleGlobalConnectionError(e);

                if (timeoutChecker.apply(System.currentTimeMillis()) == Boolean.TRUE) {
                    String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                            maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                }
            } catch (MilvusClientException e) {
                if (timeoutChecker.apply(System.currentTimeMillis()) == Boolean.TRUE) {
                    String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                            maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                }

                if (retryConfig.isRetryOnRateLimit() &&
                        (e.getLegacyServerCode() == io.milvus.grpc.ErrorCode.RateLimit.getNumber() ||
                                e.getServerErrCode() == 8)) {
                    // for server-side returned error, only retry for rate limit
                    // in new error codes of v2.3, rate limit error value is 8
                } else if (handleGlobalRoutingError(e)) {
                    // for global cluster routing errors, immediately trigger topology refresh and continue to retry
                } else {
                    throw e; // exit retry, throw the error
                }
            } catch (Exception e) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e); // others error treated as client error
            }

            if (k >= maxRetryTimes) {
                // finish retry loop, return the response of the last retry
                String msg = String.format("Finish %d retry times, stop retry", maxRetryTimes);
                logger.warn(msg);
                throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exceed max time, exit retry
            } else {
                // check if sleep would exceed maxRetryTimeoutMs, if so, directly throw timeout
                long futureTimePoint = System.currentTimeMillis() + retryIntervalMs;
                if (timeoutChecker.apply(futureTimePoint) == Boolean.TRUE) {
                    String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, "
                                    + "elapsed time + next interval %dms would exceed timeout",
                            maxRetryTimeoutMs, maxRetryTimes, k, retryIntervalMs);
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.TIMEOUT, msg);
                }

                // sleep for interval
                // print log, follow the pymilvus logic
                if (k > 3) {
                    logger.warn(String.format("Retry(%d) with interval %dms", k, retryIntervalMs));
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    String msg = String.format("Retry sleep interrupted, aborting retry after %d attempts", k);
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.CLIENT_ERROR, msg);
                }
            }

            // reset the next interval value
            retryIntervalMs = retryIntervalMs * retryConfig.getBackOffMultiplier();
            if (retryIntervalMs > retryConfig.getMaxBackOffMs()) {
                retryIntervalMs = retryConfig.getMaxBackOffMs();
            }
        }

        return null;
    }

    /**
     * Executes an asynchronous RPC with the same retry policy used by {@link #retry(Callable)}.
     * Validation failures are returned as exceptionally completed futures, retry backoff never
     * blocks a caller thread, and cancellation is propagated to the active RPC or scheduled retry.
     */
    public <T> CompletableFuture<T> retryAsync(Supplier<CompletableFuture<T>> supplier) {
        RetryFuture<T> result = new RetryFuture<>();
        int maxRetryTimes = retryConfig.getMaxRetryTimes();
        int effectiveMaxRetryTimes = Math.max(1, maxRetryTimes);
        attemptAsync(supplier, result, System.currentTimeMillis(), effectiveMaxRetryTimes,
                1, retryConfig.getInitialBackOffMs());
        return result;
    }

    private <T> void attemptAsync(Supplier<CompletableFuture<T>> supplier,
                                  RetryFuture<T> result,
                                  long begin,
                                  int maxRetryTimes,
                                  int attemptNumber,
                                  long retryIntervalMs) {
        if (result.isDone()) {
            return;
        }

        CompletableFuture<T> attempt;
        try {
            attempt = supplier.get();
            if (attempt == null) {
                throw new NullPointerException("Async RPC supplier returned null future");
            }
        } catch (Throwable throwable) {
            handleAsyncFailure(supplier, result, begin, maxRetryTimes, attemptNumber,
                    retryIntervalMs, throwable);
            return;
        }

        result.setInFlight(attempt);
        attempt.whenComplete((value, throwable) -> {
            result.clearInFlight(attempt);
            if (result.isDone()) {
                return;
            }
            if (throwable == null) {
                result.complete(value);
            } else {
                handleAsyncFailure(supplier, result, begin, maxRetryTimes, attemptNumber,
                        retryIntervalMs, unwrapCompletionThrowable(throwable));
            }
        });
    }

    private <T> void handleAsyncFailure(Supplier<CompletableFuture<T>> supplier,
                                        RetryFuture<T> result,
                                        long begin,
                                        int maxRetryTimes,
                                        int attemptNumber,
                                        long retryIntervalMs,
                                        Throwable throwable) {
        if (result.isDone()) {
            return;
        }

        Throwable cause = unwrapCompletionThrowable(throwable);
        if (maxRetryTimes <= 1) {
            result.completeExceptionally(normalizeAsyncFailure(cause));
            return;
        }
        boolean retryable;
        if (cause instanceof StatusRuntimeException) {
            StatusRuntimeException statusException = (StatusRuntimeException) cause;
            if (isNonRetryableRpcError(statusException)) {
                String msg = String.format("Encounter rpc error that cannot be retried, reason: %s",
                        statusException);
                logger.error(msg);
                result.completeExceptionally(new MilvusClientException(ErrorCode.RPC_ERROR, statusException));
                return;
            }
            handleGlobalConnectionError(statusException);
            retryable = true;
        } else if (cause instanceof MilvusClientException) {
            MilvusClientException clientException = (MilvusClientException) cause;
            retryable = retryConfig.isRetryOnRateLimit()
                    && (clientException.getLegacyServerCode() == io.milvus.grpc.ErrorCode.RateLimit.getNumber()
                    || clientException.getServerErrCode() == 8);
            if (!retryable) {
                retryable = handleGlobalRoutingError(clientException);
            }
            if (!retryable) {
                result.completeExceptionally(clientException);
                return;
            }
        } else if (cause instanceof Error) {
            result.completeExceptionally(cause);
            return;
        } else {
            result.completeExceptionally(new MilvusClientException(ErrorCode.CLIENT_ERROR, cause));
            return;
        }

        long now = System.currentTimeMillis();
        if (retryTimedOut(begin, now)) {
            completeAsyncTimeout(result, String.format(
                    "Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                    retryConfig.getMaxRetryTimeoutMs(), maxRetryTimes, attemptNumber, cause.getMessage()));
            return;
        }
        if (attemptNumber >= maxRetryTimes) {
            completeAsyncTimeout(result,
                    String.format("Finish %d retry times, stop retry", maxRetryTimes));
            return;
        }

        long futureTimePoint = now + retryIntervalMs;
        if (retryTimedOut(begin, futureTimePoint)) {
            completeAsyncTimeout(result, String.format(
                    "Retry timeout: %dms, maxRetry:%d, retries: %d, "
                            + "elapsed time + next interval %dms would exceed timeout",
                    retryConfig.getMaxRetryTimeoutMs(), maxRetryTimes, attemptNumber, retryIntervalMs));
            return;
        }

        if (attemptNumber > 3) {
            logger.warn(String.format("Retry(%d) with interval %dms", attemptNumber, retryIntervalMs));
        }
        long nextRetryIntervalMs = retryIntervalMs * retryConfig.getBackOffMultiplier();
        if (nextRetryIntervalMs > retryConfig.getMaxBackOffMs()) {
            nextRetryIntervalMs = retryConfig.getMaxBackOffMs();
        }
        long nextInterval = nextRetryIntervalMs;
        ScheduledRetry scheduledRetry = result.registerScheduled();
        try {
            ScheduledFuture<?> scheduled = asyncRetryExecutor().schedule(() -> {
                        if (result.startScheduled(scheduledRetry)) {
                            attemptAsync(supplier, result, begin, maxRetryTimes,
                                    attemptNumber + 1, nextInterval);
                        }
                    },
                    retryIntervalMs,
                    TimeUnit.MILLISECONDS);
            scheduledRetry.setFuture(scheduled);
        } catch (RuntimeException schedulingFailure) {
            result.clearScheduled(scheduledRetry);
            result.completeExceptionally(
                    new MilvusClientException(ErrorCode.CLIENT_ERROR, schedulingFailure));
        }
    }

    private boolean retryTimedOut(long begin, long timePoint) {
        long maxRetryTimeoutMs = retryConfig.getMaxRetryTimeoutMs();
        return maxRetryTimeoutMs > 0 && timePoint - begin >= maxRetryTimeoutMs;
    }

    private Throwable normalizeAsyncFailure(Throwable cause) {
        if (cause instanceof StatusRuntimeException) {
            return new MilvusClientException(ErrorCode.RPC_ERROR, cause);
        }
        if (cause instanceof MilvusClientException) {
            return cause;
        }
        if (cause instanceof Error) {
            return cause;
        }
        return new MilvusClientException(ErrorCode.CLIENT_ERROR, cause);
    }

    private boolean isNonRetryableRpcError(StatusRuntimeException exception) {
        io.grpc.Status.Code code = exception.getStatus().getCode();
        return code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                || code == io.grpc.Status.Code.PERMISSION_DENIED
                || code == io.grpc.Status.Code.UNAUTHENTICATED
                || code == io.grpc.Status.Code.INVALID_ARGUMENT
                || code == io.grpc.Status.Code.ALREADY_EXISTS
                || code == io.grpc.Status.Code.RESOURCE_EXHAUSTED
                || code == io.grpc.Status.Code.UNIMPLEMENTED;
    }

    private void completeAsyncTimeout(CompletableFuture<?> result, String message) {
        logger.warn(message);
        result.completeExceptionally(new MilvusClientException(ErrorCode.TIMEOUT, message));
    }

    private Throwable unwrapCompletionThrowable(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class RetryFuture<T> extends CompletableFuture<T> {
        private final AtomicReference<CompletableFuture<?>> inFlight = new AtomicReference<>();
        private final AtomicReference<ScheduledRetry> scheduled = new AtomicReference<>();

        private void setInFlight(CompletableFuture<?> future) {
            inFlight.set(future);
            if (isCancelled()) {
                future.cancel(true);
            }
        }

        private void clearInFlight(CompletableFuture<?> future) {
            inFlight.compareAndSet(future, null);
        }

        private ScheduledRetry registerScheduled() {
            ScheduledRetry retry = new ScheduledRetry();
            ScheduledRetry previous = scheduled.getAndSet(retry);
            if (previous != null) {
                previous.cancel();
            }
            if (isDone() && scheduled.compareAndSet(retry, null)) {
                retry.cancel();
            }
            return retry;
        }

        private boolean startScheduled(ScheduledRetry retry) {
            return scheduled.compareAndSet(retry, null) && !isDone();
        }

        private void clearScheduled(ScheduledRetry retry) {
            if (scheduled.compareAndSet(retry, null)) {
                retry.cancel();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                CompletableFuture<?> active = inFlight.getAndSet(null);
                if (active != null) {
                    active.cancel(mayInterruptIfRunning);
                }
                ScheduledRetry pending = scheduled.getAndSet(null);
                if (pending != null) {
                    pending.cancel();
                }
            }
            return cancelled;
        }
    }

    private static final class ScheduledRetry {
        private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>();
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private void setFuture(ScheduledFuture<?> scheduledFuture) {
            future.set(scheduledFuture);
            if (cancelled.get()) {
                scheduledFuture.cancel(false);
            }
        }

        private void cancel() {
            cancelled.set(true);
            ScheduledFuture<?> scheduledFuture = future.get();
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }
}
