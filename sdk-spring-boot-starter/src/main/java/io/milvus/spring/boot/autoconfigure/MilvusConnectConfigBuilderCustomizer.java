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

package io.milvus.spring.boot.autoconfigure;

import io.milvus.v2.client.ConnectConfig;

/**
 * Callback for customizing the auto-configured {@link ConnectConfig} beyond what
 * {@link MilvusProperties} exposes, for example to set an {@code SSLContext} or telemetry options.
 * <p>
 * Customizers are applied after the {@code milvus.*} properties, in
 * {@link org.springframework.core.annotation.Order @Order} order.
 */
@FunctionalInterface
public interface MilvusConnectConfigBuilderCustomizer {

    /**
     * Customizes the given builder.
     *
     * @param builder the builder to customize
     */
    void customize(ConnectConfig.ConnectConfigBuilder builder);
}
