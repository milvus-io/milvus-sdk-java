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
import java.util.concurrent.TimeUnit;

public class RpcUtils {

    protected static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);
    private RetryConfig retryConfig = RetryConfig.builder().build();

    public void retryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
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
        Callable<Boolean> timeoutChecker = ()->{
            long current = System.currentTimeMillis();
            long cost = (current - begin);
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
                    throw new MilvusClientException(ErrorCode.RPC_ERROR, msg); // throw rpc error
                }

                try {
                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                        logger.warn(msg);
                        throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                    }
                } catch (Exception ignored) {
                }
            } catch (MilvusClientException e) {
                try {
                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                        logger.warn(msg);
                        throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                    }
                } catch (Exception ignored) {
                }

                // for server-side returned error, only retry for rate limit
                // in new error codes of v2.3, rate limit error value is 8
                if (retryConfig.isRetryOnRateLimit() &&
                        (e.getLegacyServerCode() == io.milvus.grpc.ErrorCode.RateLimit.getNumber() ||
                                e.getServerErrCode() == 8)) {
                    // cannot be retried
                } else {
                    throw e; // exit retry, throw the error
                }
            } catch (Exception e) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e); // others error treated as client error
            }

            try {
                if (k >= maxRetryTimes) {
                    // finish retry loop, return the response of the last retry
                    String msg = String.format("Finish %d retry times, stop retry", maxRetryTimes);
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exceed max time, exit retry
                } else {
                    // sleep for interval
                    // print log, follow the pymilvus logic
                    if (k > 3) {
                        logger.warn(String.format("Retry(%d) with interval %dms", k, retryIntervalMs));
                    }
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
                }

                // reset the next interval value
                retryIntervalMs = retryIntervalMs*retryConfig.getBackOffMultiplier();
                if (retryIntervalMs > retryConfig.getMaxBackOffMs()) {
                    retryIntervalMs = retryConfig.getMaxBackOffMs();
                }
            } catch (Exception ignored) {
            }
        }

        return null;
    }
}
