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

import java.time.Duration;
import java.util.function.Consumer;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for {@link MilvusClientV2}.
 * <p>
 * Creates a {@link ConnectConfig} from {@link MilvusProperties} and a {@link MilvusClientV2} that is closed
 * when the application context shuts down. Both beans back off if the application defines its own.
 */
@AutoConfiguration
@ConditionalOnClass(MilvusClientV2.class)
@EnableConfigurationProperties(MilvusProperties.class)
public class MilvusAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConnectConfig milvusConnectConfig(MilvusProperties properties,
                                             ObjectProvider<MilvusConnectConfigBuilderCustomizer> customizers) {
        ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder().uri(properties.getUri());
        // only override the SDK defaults for properties that were explicitly set
        setIfNotNull(properties.getToken(), builder::token);
        setIfNotNull(properties.getUsername(), builder::username);
        setIfNotNull(properties.getPassword(), builder::password);
        setIfNotNull(properties.getDbName(), builder::dbName);
        setMillisIfNotNull(properties.getConnectTimeout(), builder::connectTimeoutMs);
        setMillisIfNotNull(properties.getKeepAliveTime(), builder::keepAliveTimeMs);
        setMillisIfNotNull(properties.getKeepAliveTimeout(), builder::keepAliveTimeoutMs);
        setIfNotNull(properties.getKeepAliveWithoutCalls(), builder::keepAliveWithoutCalls);
        setMillisIfNotNull(properties.getRpcDeadline(), builder::rpcDeadlineMs);
        setMillisIfNotNull(properties.getIdleTimeout(), builder::idleTimeoutMs);
        setIfNotNull(properties.getSecure(), builder::secure);
        setIfNotNull(properties.getServerName(), builder::serverName);
        setIfNotNull(properties.getServerPemPath(), builder::serverPemPath);
        setIfNotNull(properties.getCaPemPath(), builder::caPemPath);
        setIfNotNull(properties.getClientPemPath(), builder::clientPemPath);
        setIfNotNull(properties.getClientKeyPath(), builder::clientKeyPath);
        setIfNotNull(properties.getProxyAddress(), builder::proxyAddress);
        setIfNotNull(properties.getEnablePrecheck(), builder::enablePrecheck);
        customizers.orderedStream().forEach(customizer -> customizer.customize(builder));
        return builder.build();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public MilvusClientV2 milvusClient(ConnectConfig connectConfig) {
        return new MilvusClientV2(connectConfig);
    }

    private static <T> void setIfNotNull(T value, Consumer<T> setter) {
        if (value != null) {
            setter.accept(value);
        }
    }

    private static void setMillisIfNotNull(Duration value, Consumer<Long> setter) {
        if (value != null) {
            setter.accept(value.toMillis());
        }
    }
}
