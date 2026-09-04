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

import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.utils.RpcUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class RpcRetryConfigTest {

    @Test
    void testRetryConfigRejectsNegativeBackoffValues() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetryConfig.builder().initialBackOffMs(-1).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetryConfig.builder().maxBackOffMs(-1).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetryConfig.builder().backOffMultiplier(0).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetryConfig.builder().backOffMultiplier(-3).build());
    }

    @Test
    void testRetryConfigSettersRejectNegativeBackoffValues() {
        RetryConfig config = RetryConfig.builder().build();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> config.setInitialBackOffMs(-1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> config.setMaxBackOffMs(-1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> config.setBackOffMultiplier(0));
    }

    @Test
    void testRetrySyncAndAsyncRejectNegativeBackoffAtInstallation() {
        // parity: sync retry() used to reject a negative backoff via Thread.sleep,
        // while retryAsync() hot-looped because ScheduledThreadPoolExecutor treats a
        // negative delay as immediately due. Validation now happens in RetryConfig, the
        // shared entry point, so neither path can install a malformed config.
        RpcUtils rpcUtils = new RpcUtils();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rpcUtils.retryConfig(RetryConfig.builder().initialBackOffMs(-5).build()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rpcUtils.retryConfig(RetryConfig.builder().maxBackOffMs(-5).build()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rpcUtils.retryConfig(RetryConfig.builder().backOffMultiplier(0).build()));
    }
}
