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

package io.milvus.v2.client;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class RetryConfig {
    @Builder.Default
    private int maxRetryTimes = 75;
    @Builder.Default
    private long initialBackOffMs = 10;
    @Builder.Default
    private long maxBackOffMs = 3000;
    @Builder.Default
    private int backOffMultiplier = 3;
    @Builder.Default
    private boolean retryOnRateLimit = true;
    @Builder.Default
    private long maxRetryTimeoutMs = 0;
}
