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

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.*;
import io.grpc.stub.MetadataUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientUtils {
    Logger logger = LoggerFactory.getLogger(ClientUtils.class);
    RpcUtils rpcUtils = new RpcUtils();

    public ManagedChannel getChannel(ConnectConfig connectConfig) {
        ManagedChannel channel = null;

        Metadata metadata = new Metadata();
        if (connectConfig.getAuthorization() != null) {
            metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), Base64.getEncoder().encodeToString(connectConfig.getAuthorization().getBytes(StandardCharsets.UTF_8)));
        }
        if (StringUtils.isNotEmpty(connectConfig.getDbName())) {
            metadata.put(Metadata.Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER), connectConfig.getDbName());
        }

        List<ClientInterceptor> clientInterceptors = new ArrayList<>();
        clientInterceptors.add(MetadataUtils.newAttachHeadersInterceptor(metadata));
        //client interceptor used to fetch client_request_id from threadlocal variable and set it for every grpc request
        clientInterceptors.add(new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return new ForwardingClientCall
                        .SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                        String currentMs = String.valueOf(System.currentTimeMillis());
                        headers.put(Metadata.Key.of("client-request-unixmsec", Metadata.ASCII_STRING_MARSHALLER), currentMs);
                        if (connectConfig.getClientRequestId() != null) {
                            String clientID = connectConfig.getClientRequestId().get();
                            if (!StringUtils.isEmpty(clientID)) {
                                headers.put(Metadata.Key.of("client_request_id", Metadata.ASCII_STRING_MARSHALLER), clientID);
                            }
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        });

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
                        .intercept(clientInterceptors);

                if (StringUtils.isNotEmpty(connectConfig.getProxyAddress())) {
                    configureProxy(builder, connectConfig.getProxyAddress());
                }

                if (connectConfig.isSecure()) {
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
                        .intercept(clientInterceptors);

                if (StringUtils.isNotEmpty(connectConfig.getProxyAddress())) {
                    configureProxy(builder, connectConfig.getProxyAddress());
                }

                if (connectConfig.isSecure()) {
                    builder.useTransportSecurity();
                }
                channel = builder.build();
            } else if (StringUtils.isNotEmpty(connectConfig.getClientPemPath())
                    && StringUtils.isNotEmpty(connectConfig.getClientKeyPath())
                    && StringUtils.isNotEmpty(connectConfig.getCaPemPath())) {
                // two-way tls
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
                        .intercept(clientInterceptors);

                if (StringUtils.isNotEmpty(connectConfig.getProxyAddress())) {
                    configureProxy(builder, connectConfig.getProxyAddress());
                }

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
                        .intercept(clientInterceptors);
                if (StringUtils.isNotEmpty(connectConfig.getProxyAddress())) {
                    configureProxy(builder, connectConfig.getProxyAddress());
                }
                if (connectConfig.isSecure()) {
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

    /**
     * Configures the proxy settings for a NettyChannelBuilder if proxy address is specified
     *
     * @param builder      NettyChannelBuilder to configure
     * @param proxyAddress proxy address
     */
    public static void configureProxy(ManagedChannelBuilder builder, String proxyAddress) {
        String[] hostPort = proxyAddress.split(":");
        if (hostPort.length == 2) {
            String proxyHost = hostPort[0];
            int proxyPort = Integer.parseInt(hostPort[1]);

            builder.proxyDetector(new ProxyDetector() {
                @Override
                public ProxiedSocketAddress proxyFor(SocketAddress targetServerAddress) {
                    return HttpConnectProxiedSocketAddress.newBuilder()
                            .setProxyAddress(new InetSocketAddress(proxyHost, proxyPort))
                            .setTargetAddress((InetSocketAddress) targetServerAddress)
                            .build();
                }
            });
        }
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

    /**
     * Validates that the hostname can be resolved before attempting connection.
     * This provides early failure with clear error messages for DNS issues.
     *
     * @param connectConfig Connection configuration containing the host to validate
     * @throws MilvusClientException if hostname cannot be resolved
     */
    public void validateHostname(ConnectConfig connectConfig) {
        String host = connectConfig.getHost();

        if (StringUtils.isEmpty(host)) {
            throw new MilvusClientException(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS,
                    "Hostname cannot be null or empty");
        }

        try {
            // Attempt DNS resolution
            InetAddress.getByName(host);
            logger.debug("Successfully resolved hostname: {}", host);
        } catch (UnknownHostException e) {
            String message = String.format(
                    "Failed to resolve hostname '%s'. Please verify the hostname is correct and DNS is configured properly.",
                    host
            );
            logger.error(message, e);
            throw new MilvusClientException(io.milvus.v2.exception.ErrorCode.RPC_ERROR, message);
        }
    }

    /**
     * Validates port number and tests connectivity.
     *
     * @param connectConfig Connection configuration containing the port to validate
     * @throws MilvusClientException if port is invalid or unreachable
     */
    public void validatePort(ConnectConfig connectConfig) {
        int port = connectConfig.getPort();
        String host = connectConfig.getHost();

        // Check valid range
        if (port < 1 || port > 65535) {
            String message = String.format(
                    "Invalid port number '%d'. Port must be between 1 and 65535.",
                    port
            );
            logger.error(message);
            throw new MilvusClientException(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, message);
        }

        // Test if port is reachable
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), (int) connectConfig.getConnectTimeoutMs());
            logger.debug("Successfully validated port: {}", port);
        } catch (IOException e) {
            String message = String.format(
                    "Cannot connect to '%s:%d'. Please verify the port number is correct and server is running.",
                    host, port
            );
            logger.error(message, e);
            throw new MilvusClientException(io.milvus.v2.exception.ErrorCode.RPC_ERROR, message);
        }
    }

    /**
     * Validates SSL connection with certificates.
     *
     * @param connectConfig Connection configuration
     * @throws MilvusClientException if SSL connection fails
     */
    public void validateCert(ConnectConfig connectConfig) {
        if (!connectConfig.isSecure()) {
            return;
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            TrustManagerFactory tmf = null;

            // Load server certificate (CA cert)
            if (connectConfig.getServerPemPath() != null && !connectConfig.getServerPemPath().isEmpty()) {
                try (InputStream certStream = new FileInputStream(connectConfig.getServerPemPath())) {
                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    X509Certificate caCert = (X509Certificate) cf.generateCertificate(certStream);

                    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    trustStore.load(null, null);
                    trustStore.setCertificateEntry("ca-cert", caCert);

                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                }
            }

            // Initialize SSLContext with the server certificate
            sslContext.init(null, tmf != null ? tmf.getTrustManagers() : null, new SecureRandom());

            // Validate connection
            SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            try (SSLSocket socket = (SSLSocket) socketFactory.createSocket()) {
                socket.connect(new InetSocketAddress(connectConfig.getHost(), connectConfig.getPort()),
                        (int) connectConfig.getConnectTimeoutMs());
                socket.startHandshake();
                logger.debug("SSL certificate validation passed");
            }

        } catch (SSLException e) {
            throw new MilvusClientException(io.milvus.v2.exception.ErrorCode.RPC_ERROR,
                    "SSL certificate validation failed: " + e.getMessage() +
                            ". Please verify your certificates are correct.");
        } catch (Exception e) {
            throw new MilvusClientException(ErrorCode.RPC_ERROR,
                    "Failed to connect with SSL: " + e.getMessage());
        }
    }
}
