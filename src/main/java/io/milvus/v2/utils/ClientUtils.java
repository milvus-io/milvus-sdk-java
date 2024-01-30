package io.milvus.v2.utils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.v2.client.ConnectConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class ClientUtils {
    Logger logger = LoggerFactory.getLogger(ClientUtils.class);
    RpcUtils rpcUtils = new RpcUtils();
    public ManagedChannel getChannel(ConnectConfig connectConfig){
        ManagedChannel channel = null;

        Metadata metadata = new Metadata();

        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), Base64.getEncoder().encodeToString(connectConfig.getAuthorization().getBytes(StandardCharsets.UTF_8)));
        if (StringUtils.isNotEmpty(connectConfig.getDbName())) {
            metadata.put(Metadata.Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER), connectConfig.getDbName());
        }

        try {
            if (StringUtils.isNotEmpty(connectConfig.getServerPemPath())) {
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
                if(connectConfig.isSecure()){
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

    public void checkDatabaseExist(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName) {
        String title = String.format("Check database %s exist", dbName);
        ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder().build();
        ListDatabasesResponse response = blockingStub.listDatabases(listDatabasesRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        if (!response.getDbNamesList().contains(dbName)) {
            throw new IllegalArgumentException("Database " + dbName + " not exist");
        }
    }
}
