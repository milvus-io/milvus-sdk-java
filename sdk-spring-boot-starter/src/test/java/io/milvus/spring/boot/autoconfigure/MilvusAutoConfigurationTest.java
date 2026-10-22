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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;

@Tag("unit")
class MilvusAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MilvusAutoConfiguration.class));

    // MilvusClientV2 connects in its constructor. With lazy initialization (what spring.main.lazy-initialization
    // enables) the client is only created on first use, so tests that inspect the ConnectConfig or the bean
    // definitions do not need a running server
    private final ApplicationContextRunner lazyContextRunner = contextRunner
            .withBean(LazyInitializationBeanFactoryPostProcessor.class);

    @Test
    void shouldBeRegisteredAsAutoConfiguration() {
        ImportCandidates candidates = ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader());

        assertThat(candidates.getCandidates()).contains(MilvusAutoConfiguration.class.getName());
    }

    @Test
    void shouldUseSdkDefaultsWhenNoPropertiesAreSet() {
        lazyContextRunner.run(context -> {
            ConnectConfig config = context.getBean(ConnectConfig.class);
            ConnectConfig sdkDefaults = ConnectConfig.builder().uri(MilvusProperties.DEFAULT_URI).build();

            assertThat(config.getUri()).isEqualTo(MilvusProperties.DEFAULT_URI);
            assertThat(config.getToken()).isNull();
            assertThat(config.getDbName()).isNull();
            assertThat(config.getConnectTimeoutMs()).isEqualTo(sdkDefaults.getConnectTimeoutMs());
            assertThat(config.getKeepAliveTimeMs()).isEqualTo(sdkDefaults.getKeepAliveTimeMs());
            assertThat(config.getKeepAliveTimeoutMs()).isEqualTo(sdkDefaults.getKeepAliveTimeoutMs());
            assertThat(config.isKeepAliveWithoutCalls()).isEqualTo(sdkDefaults.isKeepAliveWithoutCalls());
            assertThat(config.getRpcDeadlineMs()).isEqualTo(sdkDefaults.getRpcDeadlineMs());
            assertThat(config.getIdleTimeoutMs()).isEqualTo(sdkDefaults.getIdleTimeoutMs());
            assertThat(config.getSecure()).isEqualTo(sdkDefaults.getSecure());
            assertThat(config.isEnablePrecheck()).isEqualTo(sdkDefaults.isEnablePrecheck());
        });
    }

    @Test
    void shouldBindAllPropertiesToConnectConfig() {
        lazyContextRunner
                .withPropertyValues(
                        "milvus.uri=https://milvus.example.com:443",
                        "milvus.token=root:Milvus",
                        "milvus.username=alice",
                        "milvus.password=secret",
                        "milvus.db-name=rag",
                        "milvus.connect-timeout=3s",
                        "milvus.keep-alive-time=20s",
                        "milvus.keep-alive-timeout=7s",
                        "milvus.keep-alive-without-calls=false",
                        "milvus.rpc-deadline=500ms",
                        "milvus.idle-timeout=1h",
                        "milvus.secure=true",
                        "milvus.server-name=milvus.example.com",
                        "milvus.server-pem-path=/certs/server.pem",
                        "milvus.ca-pem-path=/certs/ca.pem",
                        "milvus.client-pem-path=/certs/client.pem",
                        "milvus.client-key-path=/certs/client.key",
                        "milvus.proxy-address=proxy.example.com:8080",
                        "milvus.enable-precheck=true")
                .run(context -> {
                    ConnectConfig config = context.getBean(ConnectConfig.class);

                    assertThat(config.getUri()).isEqualTo("https://milvus.example.com:443");
                    assertThat(config.getToken()).isEqualTo("root:Milvus");
                    assertThat(config.getUsername()).isEqualTo("alice");
                    assertThat(config.getPassword()).isEqualTo("secret");
                    assertThat(config.getDbName()).isEqualTo("rag");
                    assertThat(config.getConnectTimeoutMs()).isEqualTo(3_000L);
                    assertThat(config.getKeepAliveTimeMs()).isEqualTo(20_000L);
                    assertThat(config.getKeepAliveTimeoutMs()).isEqualTo(7_000L);
                    assertThat(config.isKeepAliveWithoutCalls()).isFalse();
                    assertThat(config.getRpcDeadlineMs()).isEqualTo(500L);
                    assertThat(config.getIdleTimeoutMs()).isEqualTo(TimeUnit.HOURS.toMillis(1));
                    assertThat(config.getSecure()).isTrue();
                    assertThat(config.getServerName()).isEqualTo("milvus.example.com");
                    assertThat(config.getServerPemPath()).isEqualTo("/certs/server.pem");
                    assertThat(config.getCaPemPath()).isEqualTo("/certs/ca.pem");
                    assertThat(config.getClientPemPath()).isEqualTo("/certs/client.pem");
                    assertThat(config.getClientKeyPath()).isEqualTo("/certs/client.key");
                    assertThat(config.getProxyAddress()).isEqualTo("proxy.example.com:8080");
                    assertThat(config.isEnablePrecheck()).isTrue();
                });
    }

    @Test
    void shouldApplyCustomizersInOrderAfterProperties() {
        lazyContextRunner
                .withPropertyValues("milvus.db-name=from-properties")
                .withUserConfiguration(CustomizerConfiguration.class)
                .run(context -> {
                    ConnectConfig config = context.getBean(ConnectConfig.class);

                    // the customizer with the higher order runs last and wins
                    assertThat(config.getDbName()).isEqualTo("from-second-customizer");
                    assertThat(config.getServerName()).isEqualTo("from-first-customizer");
                });
    }

    @Test
    void shouldBackOffWhenConnectConfigIsDefinedByUser() {
        ConnectConfig userConfig = ConnectConfig.builder().uri("http://user-defined:19530").build();

        lazyContextRunner
                .withPropertyValues("milvus.uri=http://from-properties:19530")
                .withBean(ConnectConfig.class, () -> userConfig)
                .run(context -> {
                    assertThat(context).hasSingleBean(ConnectConfig.class);
                    assertThat(context.getBean(ConnectConfig.class)).isSameAs(userConfig);
                });
    }

    @Test
    void shouldBackOffWhenClientIsDefinedByUser() {
        lazyContextRunner
                .withBean("userMilvusClient", MilvusClientV2.class, () -> {
                    throw new IllegalStateException("the client is never created in this test");
                })
                .run(context -> {
                    assertThat(context).hasSingleBean(MilvusClientV2.class);
                    assertThat(context.getBeanNamesForType(MilvusClientV2.class)).containsExactly("userMilvusClient");
                });
    }

    @Test
    void shouldTreatBlankPropertiesAsUnset() {
        lazyContextRunner
                .withPropertyValues(
                        "milvus.token=",
                        "milvus.username=",
                        "milvus.password= ",
                        "milvus.db-name=",
                        "milvus.server-name=",
                        "milvus.proxy-address=")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ConnectConfig config = context.getBean(ConnectConfig.class);

                    assertThat(config.getToken()).isNull();
                    assertThat(config.getUsername()).isNull();
                    assertThat(config.getPassword()).isNull();
                    assertThat(config.getDbName()).isNull();
                    assertThat(config.getServerName()).isNull();
                    assertThat(config.getProxyAddress()).isNull();
                });
    }

    @Test
    void shouldFailWithClearMessageWhenUriIsBlank() {
        contextRunner
                .withPropertyValues("milvus.uri= ")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("milvus.uri must not be empty"));
    }

    @Test
    void shouldNotConfigureAnythingWhenDisabled() {
        contextRunner
                .withPropertyValues("milvus.enabled=false", "milvus.uri=http://127.0.0.1:1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(MilvusProperties.class);
                    assertThat(context).doesNotHaveBean(ConnectConfig.class);
                    assertThat(context).doesNotHaveBean(MilvusClientV2.class);
                });
    }

    @Test
    void shouldStartWithLazyInitializationWhenServerIsUnreachable() {
        // nothing listens on port 1; the client is not created until first use
        lazyContextRunner
                .withPropertyValues("milvus.uri=http://127.0.0.1:1", "milvus.connect-timeout=500ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(MilvusClientV2.class);
                });
    }

    @Test
    void shouldNotConfigureAnythingWhenSdkIsNotOnClasspath() {
        contextRunner
                .withClassLoader(new FilteredClassLoader(MilvusClientV2.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(MilvusProperties.class);
                    assertThat(context).doesNotHaveBean(ConnectConfig.class);
                    assertThat(context).doesNotHaveBean(MilvusClientV2.class);
                });
    }

    @Test
    void shouldFailFastByDefaultWhenServerIsUnreachable() {
        // nothing listens on port 1, so the eagerly created client cannot connect
        contextRunner
                .withPropertyValues("milvus.uri=http://127.0.0.1:1", "milvus.connect-timeout=500ms")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .hasMessageContaining("milvusClient"));
    }

    static class CustomizerConfiguration {

        @Bean
        @Order(2)
        MilvusConnectConfigBuilderCustomizer secondCustomizer() {
            return builder -> builder.dbName("from-second-customizer");
        }

        @Bean
        @Order(1)
        MilvusConnectConfigBuilderCustomizer firstCustomizer() {
            return builder -> builder.dbName("from-first-customizer").serverName("from-first-customizer");
        }
    }
}
