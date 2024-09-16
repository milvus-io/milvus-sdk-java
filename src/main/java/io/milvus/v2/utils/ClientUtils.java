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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.grpc.netty.shaded.io.netty.handler.ssl.JdkSslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.MetadataUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.v2.client.ConnectConfig;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class ClientUtils {
    Logger logger = LoggerFactory.getLogger(ClientUtils.class);
    RpcUtils rpcUtils = new RpcUtils();
    public ManagedChannel getChannel(ConnectConfig connectConfig){
        ManagedChannel channel = null;

        Metadata metadata = new Metadata();
        if (connectConfig.getAuthorization() != null) {
            metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), Base64.getEncoder().encodeToString(connectConfig.getAuthorization().getBytes(StandardCharsets.UTF_8)));
        }
        if (StringUtils.isNotEmpty(connectConfig.getDbName())) {
            metadata.put(Metadata.Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER), connectConfig.getDbName());
        }

        try {
            if (connectConfig.getSslContext() != null) {
                // sslContext from connect config
                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(connectConfig.getHost(), connectConfig.getPort())
                        .overrideAuthority(connectConfig.getServerName())
                        .sslContext(convertJavaSslContextToNetty(connectConfig))
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                        .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
                if(connectConfig.isSecure()) {
                    builder.useTransportSecurity();
                }
                if (StringUtils.isNotEmpty(connectConfig.getServerName())) {
                    builder.overrideAuthority(connectConfig.getServerName());
                }
                channel = builder.build();
            } else if (StringUtils.isNotEmpty(connectConfig.getServerPemPath())) {
                // one-way tls
                SslContext sslContext = GrpcSslContexts.forClient()
                        .trustManager(new File(connectConfig.getServerPemPath()))
                        .build();

                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(connectConfig.getHost(), connectConfig.getPort())
                        .overrideAuthority(connectConfig.getServerName())
                        .sslContext(sslContext)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                        .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
                if(connectConfig.isSecure()){
                    builder.useTransportSecurity();
                }
                channel = builder.build();
            } else if (StringUtils.isNotEmpty(connectConfig.getClientPemPath())
                    && StringUtils.isNotEmpty(connectConfig.getClientKeyPath())
                    && StringUtils.isNotEmpty(connectConfig.getCaPemPath())) {
                // tow-way tls
                SslContext sslContext = GrpcSslContexts.forClient()
                        .trustManager(new File(connectConfig.getCaPemPath()))
                        .keyManager(new File(connectConfig.getClientPemPath()), new File(connectConfig.getClientKeyPath()))
                        .build();

                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(connectConfig.getHost(), connectConfig.getPort())
                        .sslContext(sslContext)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                        .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
                if (connectConfig.getSecure()) {
                    builder.useTransportSecurity();
                }
                if (StringUtils.isNotEmpty(connectConfig.getServerName())) {
                    builder.overrideAuthority(connectConfig.getServerName());
                }
                channel = builder.build();
            } else {
                // no tls
                ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(connectConfig.getHost(), connectConfig.getPort())
                        .usePlaintext()
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                        .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
                if(connectConfig.isSecure()){
                    builder.useTransportSecurity();
                }
                channel = builder.build();
            }
        } catch (IOException e) {
            logger.error("Failed to open credentials file, error:{}\n", e.getMessage());
        }
        assert channel != null;
        return channel;
    }

    private static JdkSslContext convertJavaSslContextToNetty(ConnectConfig connectConfig) {
        ApplicationProtocolConfig applicationProtocolConfig = new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.NONE,
                ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT, ApplicationProtocolConfig.SelectedListenerFailureBehavior.FATAL_ALERT);
        return new JdkSslContext(connectConfig.getSslContext(), true, null,
                IdentityCipherSuiteFilter.INSTANCE, applicationProtocolConfig, ClientAuth.NONE, null, false);
    }

    public void checkDatabaseExist(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName) {
        String title = String.format("Check database %s exist", dbName);
        ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder().build();
        ListDatabasesResponse response = blockingStub.listDatabases(listDatabasesRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        if (!response.getDbNamesList().contains(dbName)) {
            throw new IllegalArgumentException("Database " + dbName + " not exist");
        }
    }
    public String getServerVersion(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        GetVersionResponse response = blockingStub.getVersion(GetVersionRequest.newBuilder().build());
        rpcUtils.handleResponse("Get server version", response.getStatus());
        return response.getVersion();
    }

    public String getHostName() {
        try {
            InetAddress address = InetAddress.getLocalHost();
            return address.getHostName();
        } catch (UnknownHostException e) {
            logger.warn("Failed to get host name, error:{}\n", e.getMessage());
            return "Unknown";
        }
    }

    public String getLocalTimeStr() {
        LocalDateTime now = LocalDateTime.now();
        return now.toString();
    }

    public String getSDKVersion() {
        Package pkg = MilvusServiceClient.class.getPackage();
        String ver = pkg.getImplementationVersion();
        if (ver == null) {
            return "";
        }
        return ver;
    }
}
