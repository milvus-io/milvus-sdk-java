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

package io.milvus.client;

import io.grpc.*;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.MetadataUtils;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.exception.MilvusException;
import io.milvus.exception.ServerException;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.*;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.alias.ListAliasesParam;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;
import io.milvus.param.bulkinsert.ListBulkInsertTasksParam;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.dml.*;
import io.milvus.param.highlevel.dml.response.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.param.resourcegroup.*;
import io.milvus.param.role.*;
import io.milvus.v2.utils.ClientUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class MilvusServiceClient extends AbstractMilvusGrpcClient {

    private ManagedChannel channel;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final MilvusServiceGrpc.MilvusServiceFutureStub futureStub;
    private final long rpcDeadlineMs;
    private long timeoutMs = 0;
    private RetryParam retryParam = RetryParam.newBuilder().build();
    private String currentDatabaseName;

    public MilvusServiceClient(ConnectParam connectParam) {
        ExceptionUtils.checkNotNull(connectParam, connectParam.getClass().getSimpleName());
        this.rpcDeadlineMs = connectParam.getRpcDeadlineMs();

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), connectParam.getAuthorization());
        if (StringUtils.isNotEmpty(connectParam.getDatabaseName())) {
            currentDatabaseName = connectParam.getDatabaseName();
            metadata.put(Metadata.Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER), connectParam.getDatabaseName());
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
                        if (connectParam.getClientRequestId() != null && !StringUtils.isEmpty(connectParam.getClientRequestId().get())) {
                            headers.put(Metadata.Key.of("client_request_id", Metadata.ASCII_STRING_MARSHALLER), connectParam.getClientRequestId().get());
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        });

        try {
            if (StringUtils.isNotEmpty(connectParam.getServerPemPath())) {
                // one-way tls
                SslContext sslContext = GrpcSslContexts.forClient()
                        .trustManager(new File(connectParam.getServerPemPath()))
                        .build();
                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
                        .overrideAuthority(connectParam.getServerName())
                        .sslContext(sslContext)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectParam.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectParam.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
                        .idleTimeout(connectParam.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(clientInterceptors);
                // Add proxy configuration if proxy address is set
                if (StringUtils.isNotEmpty(connectParam.getProxyAddress())) {
                    ClientUtils.configureProxy(builder, connectParam.getProxyAddress());
                }
                if (connectParam.isSecure()) {
                    builder.useTransportSecurity();
                }
                channel = builder.build();
            } else if (StringUtils.isNotEmpty(connectParam.getClientPemPath())
                    && StringUtils.isNotEmpty(connectParam.getClientKeyPath())
                    && StringUtils.isNotEmpty(connectParam.getCaPemPath())) {
                // tow-way tls
                SslContext sslContext = GrpcSslContexts.forClient()
                        .trustManager(new File(connectParam.getCaPemPath()))
                        .keyManager(new File(connectParam.getClientPemPath()), new File(connectParam.getClientKeyPath()))
                        .build();
                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
                        .sslContext(sslContext)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectParam.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectParam.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
                        .idleTimeout(connectParam.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(clientInterceptors);

                // Add proxy configuration if proxy address is set
                if (StringUtils.isNotEmpty(connectParam.getProxyAddress())) {
                    ClientUtils.configureProxy(builder, connectParam.getProxyAddress());
                }
                if (connectParam.isSecure()) {
                    builder.useTransportSecurity();
                }
                if (StringUtils.isNotEmpty(connectParam.getServerName())) {
                    builder.overrideAuthority(connectParam.getServerName());
                }
                channel = builder.build();
            } else {
                // no tls
                ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
                        .usePlaintext()
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .keepAliveTime(connectParam.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(connectParam.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
                        .idleTimeout(connectParam.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                        .intercept(clientInterceptors);
                if (StringUtils.isNotEmpty(connectParam.getProxyAddress())) {
                    ClientUtils.configureProxy(builder, connectParam.getProxyAddress());
                }
                if (connectParam.isSecure()) {
                    builder.useTransportSecurity();
                }
                channel = builder.build();
            }
        } catch (IOException e) {
            String msg = "Failed to open credentials file. Error: " + e.getMessage();
            logError(msg);
            throw new RuntimeException(msg);
        }

        assert channel != null;

        try {
            blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
            futureStub = MilvusServiceGrpc.newFutureStub(channel);

            // calls a RPC Connect() to the remote server, and sends the client info to the server
            // so that the server knows which client is interacting, especially for accesses log.
            this.timeoutMs = connectParam.getConnectTimeoutMs(); // set this value to connectTimeoutMs to control the retry()
            R<ConnectResponse> resp = this.retry(() -> connect(connectParam));
            if (resp.getStatus() != R.Status.Success.getCode()) {
                String msg = "Failed to initialize connection. Error: " + resp.getMessage();
                logError(msg);
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            // close the channel if connect() throws exception, avoid leakage
            try {
                close(3);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
        this.timeoutMs = 0; // reset the timeout value to default
    }

    protected MilvusServiceClient(MilvusServiceClient src) {
        this.channel = src.channel;
        this.blockingStub = src.blockingStub;
        this.futureStub = src.futureStub;
        this.rpcDeadlineMs = src.rpcDeadlineMs;
        this.timeoutMs = src.timeoutMs;
        this.logLevel = src.logLevel;
        this.retryParam = src.retryParam;
        this.currentDatabaseName = src.currentDatabaseName;
    }

    @Override
    protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
        if (this.rpcDeadlineMs > 0) {
            return this.blockingStub.withWaitForReady()
                    .withDeadlineAfter(this.rpcDeadlineMs, TimeUnit.MILLISECONDS);
        }
        return this.blockingStub;
    }

    @Override
    protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
        return this.futureStub;
    }

    @Override
    public boolean clientIsReady() {
        return channel != null && !channel.isShutdown() && !channel.isTerminated();
    }

    @Override
    protected String currentDbName() {
        return currentDatabaseName;
    }

    @Override
    public void close(long maxWaitSeconds) throws InterruptedException {
        channel.shutdownNow();
        channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
    }

    private static class TimeoutInterceptor implements ClientInterceptor {
        private final long timeoutMillis;

        TimeoutInterceptor(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return next.newCall(method, callOptions.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
        final long timeoutMillis = timeoutUnit.toMillis(timeout);
        final TimeoutInterceptor timeoutInterceptor = new TimeoutInterceptor(timeoutMillis);
        final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStubTimeout =
                this.blockingStub.withInterceptors(timeoutInterceptor);
        final MilvusServiceGrpc.MilvusServiceFutureStub futureStubTimeout =
                this.futureStub.withInterceptors(timeoutInterceptor);

        MilvusServiceClient newClient = new MilvusServiceClient(this) {
            @Override
            protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
                return blockingStubTimeout;
            }

            @Override
            protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
                return futureStubTimeout;
            }
        };
        newClient.timeoutMs = timeoutMillis;
        return newClient;
    }

    @Override
    public MilvusClient withRetry(RetryParam retryParam) {
        MilvusServiceClient newClient = new MilvusServiceClient(this);
        newClient.retryParam = retryParam;
        return newClient;
    }

    @Override
    public MilvusClient withRetry(int retryTimes) {
        if (retryTimes <= 0) {
            return this;
        }

        MilvusServiceClient newClient = new MilvusServiceClient(this);
        newClient.retryParam.setMaxRetryTimes(retryTimes);
        return newClient;
    }

    @Override
    public MilvusClient withRetryInterval(long interval, TimeUnit timeUnit) {
        if (interval <= 0) {
            return this;
        }

        // to compatible with the old behavior
        MilvusServiceClient newClient = new MilvusServiceClient(this);
        newClient.retryParam.setInitialBackOffMs(timeUnit.toMillis(interval));
        newClient.retryParam.setMaxBackOffMs(timeUnit.toMillis(interval));
        return newClient;
    }

    private <T> R<T> retry(Callable<R<T>> callable) {
        int maxRetryTimes = this.retryParam.getMaxRetryTimes();
        // no retry, direct call the method
        if (maxRetryTimes <= 1) {
            try {
                return callable.call();
            } catch (Exception e) {
                return R.failed(e);
            }
        }

        // method to check timeout
        long begin = System.currentTimeMillis();
        Callable<Boolean> timeoutChecker = () -> {
            long current = System.currentTimeMillis();
            long cost = (current - begin);
            if (this.timeoutMs > 0 && cost >= this.timeoutMs) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        };

        // retry within timeout
        long retryIntervalMs = this.retryParam.getInitialBackOffMs();
        for (int k = 1; k <= maxRetryTimes; k++) {
            try {
                R<T> resp = callable.call();
                if (resp.getStatus() == R.Status.Success.getCode()) {
                    return resp;
                }

                Exception e = resp.getException();
                if (e instanceof StatusRuntimeException) {
                    // for rpc exception, some error cannot be retried
                    StatusRuntimeException rpcException = (StatusRuntimeException) e;
                    Status.Code code = rpcException.getStatus().getCode();
                    if (code == Status.DEADLINE_EXCEEDED.getCode()
                            || code == Status.PERMISSION_DENIED.getCode()
                            || code == Status.UNAUTHENTICATED.getCode()
                            || code == Status.INVALID_ARGUMENT.getCode()
                            || code == Status.ALREADY_EXISTS.getCode()
                            || code == Status.RESOURCE_EXHAUSTED.getCode()
                            || code == Status.UNIMPLEMENTED.getCode()) {
                        return resp;
                    }

                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                this.timeoutMs, maxRetryTimes, k, e);
                        throw new MilvusException(msg, code.value());
                    }
                } else if (e instanceof ServerException) {
                    ServerException serverException = (ServerException) e;
                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                this.timeoutMs, maxRetryTimes, k, e);
                        throw new MilvusException(msg, serverException.getStatus());
                    }

                    // for server-side returned error, only retry for rate limit
                    // in new error codes of v2.3, rate limit error value is 8
                    if (retryParam.isRetryOnRateLimit() &&
                            (serverException.getCompatibleCode() == ErrorCode.RateLimit ||
                                    serverException.getStatus() == 8)) {
                    } else {
                        return resp;
                    }
                } else {
                    return resp;
                }

                if (k >= maxRetryTimes) {
                    // finish retry loop, return the response of the last retry
                    String msg = String.format("Finish %d retry times, stop retry", maxRetryTimes);
                    logError(msg);
                    return resp;
                } else {
                    // sleep for interval
                    // print log, follow the pymilvus logic
                    if (k > 3) {
                        logWarning(String.format("Retry(%d) with interval %dms. Reason: %s",
                                k, retryIntervalMs, e));
                    }
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
                }

                // reset the next interval value
                retryIntervalMs = retryIntervalMs * this.retryParam.getBackOffMultiplier();
                if (retryIntervalMs > this.retryParam.getMaxBackOffMs()) {
                    retryIntervalMs = this.retryParam.getMaxBackOffMs();
                }
            } catch (Exception e) {
                logError(e.getMessage());
                return R.failed(e);
            }
        }
        String msg = String.format("Finish %d retry times, stop retry", maxRetryTimes);
        logError(msg);
        return R.failed(new RuntimeException(msg));
    }

    /**
     * This method is internal used, it calls a RPC Connect() to the remote server,
     * and sends the client info to the server so that the server knows which client is interacting,
     * especially for accesses log.
     * <p>
     * The info includes:
     * 1. username(if Authentication is enabled)
     * 2. the client computer's name
     * 3. sdk language type and version
     * 4. the client's local time
     */
    private R<ConnectResponse> connect(ConnectParam connectParam) {
        ExceptionUtils.checkNotNull(connectParam, connectParam.getClass().getSimpleName());
        ClientInfo info = ClientInfo.newBuilder()
                .setSdkType("Java")
                .setSdkVersion(getSDKVersion())
                .setUser(connectParam.getUserName())
                .setHost(getHostName())
                .setLocalTime(getLocalTimeStr())
                .build();
        ConnectRequest req = ConnectRequest.newBuilder().setClientInfo(info).build();
        ConnectResponse resp = this.blockingStub.withWaitForReady()
                .withDeadlineAfter(connectParam.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .connect(req);
        if (resp.getStatus().getCode() != 0 || !resp.getStatus().getErrorCode().equals(ErrorCode.Success)) {
            throw new RuntimeException("Failed to initialize connection. Error: " + resp.getStatus().getReason());
        }
        return R.success(resp);
    }

    private String getHostName() {
        try {
            InetAddress address = InetAddress.getLocalHost();
            return address.getHostName();
        } catch (UnknownHostException e) {
            logWarning("Failed to get host name! Exception:{}", e);
            return "Unknown";
        }
    }

    private String getLocalTimeStr() {
        LocalDateTime now = LocalDateTime.now();
        return now.toString();
    }

    private String getSDKVersion() {
        Package pkg = MilvusServiceClient.class.getPackage();
        String ver = pkg.getImplementationVersion();
        if (ver == null) {
            return "";
        }
        return ver;
    }

    @Override
    public void setLogLevel(LogLevel level) {
        logLevel = level;
    }

    @Override
    public R<Boolean> hasCollection(HasCollectionParam requestParam) {
        return retry(() -> super.hasCollection(requestParam));
    }

    @Override
    public R<RpcStatus> createDatabase(CreateDatabaseParam requestParam) {
        return retry(() -> super.createDatabase(requestParam));
    }

    @Override
    public R<RpcStatus> dropDatabase(DropDatabaseParam requestParam) {
        return retry(() -> super.dropDatabase(requestParam));
    }

    @Override
    public R<ListDatabasesResponse> listDatabases() {
        return retry(super::listDatabases);
    }

    @Override
    public R<RpcStatus> alterDatabase(AlterDatabaseParam requestParam) {
        return retry(() -> super.alterDatabase(requestParam));
    }

    @Override
    public R<DescribeDatabaseResponse> describeDatabase(DescribeDatabaseParam requestParam) {
        return retry(() -> super.describeDatabase(requestParam));
    }

    @Override
    public R<RpcStatus> createCollection(CreateCollectionParam requestParam) {
        return retry(() -> super.createCollection(requestParam));
    }

    @Override
    public R<RpcStatus> dropCollection(DropCollectionParam requestParam) {
        return retry(() -> super.dropCollection(requestParam));
    }

    @Override
    public R<RpcStatus> loadCollection(LoadCollectionParam requestParam) {
        return retry(() -> super.loadCollection(requestParam));
    }

    @Override
    public R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam) {
        return retry(() -> super.releaseCollection(requestParam));
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam) {
        return retry(() -> super.describeCollection(requestParam));
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam) {
        return retry(() -> super.getCollectionStatistics(requestParam));
    }

    @Override
    public R<RpcStatus> renameCollection(RenameCollectionParam requestParam) {
        return retry(() -> super.renameCollection(requestParam));
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam) {
        return retry(() -> super.showCollections(requestParam));
    }

    @Override
    public R<RpcStatus> alterCollection(AlterCollectionParam requestParam) {
        return retry(() -> super.alterCollection(requestParam));
    }

    @Override
    public R<FlushResponse> flush(FlushParam requestParam) {
        return retry(() -> super.flush(requestParam));
    }

    @Override
    public R<FlushAllResponse> flushAll(boolean syncFlushAll, long syncFlushAllWaitingInterval, long syncFlushAllTimeout) {
        return retry(() -> super.flushAll(syncFlushAll, syncFlushAllWaitingInterval, syncFlushAllTimeout));
    }

    @Override
    public R<RpcStatus> createPartition(CreatePartitionParam requestParam) {
        return retry(() -> super.createPartition(requestParam));
    }

    @Override
    public R<RpcStatus> dropPartition(DropPartitionParam requestParam) {
        return retry(() -> super.dropPartition(requestParam));
    }

    @Override
    public R<Boolean> hasPartition(HasPartitionParam requestParam) {
        return retry(() -> super.hasPartition(requestParam));
    }

    @Override
    public R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam) {
        return retry(() -> super.loadPartitions(requestParam));
    }

    @Override
    public R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam) {
        return retry(() -> super.releasePartitions(requestParam));
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam) {
        return retry(() -> super.getPartitionStatistics(requestParam));
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam) {
        return retry(() -> super.showPartitions(requestParam));
    }

    @Override
    public R<RpcStatus> createAlias(CreateAliasParam requestParam) {
        return retry(() -> super.createAlias(requestParam));
    }

    @Override
    public R<RpcStatus> dropAlias(DropAliasParam requestParam) {
        return retry(() -> super.dropAlias(requestParam));
    }

    @Override
    public R<RpcStatus> alterAlias(AlterAliasParam requestParam) {
        return retry(() -> super.alterAlias(requestParam));
    }

    @Override
    public R<ListAliasesResponse> listAliases(ListAliasesParam requestParam) {
        return retry(() -> super.listAliases(requestParam));
    }

    @Override
    public R<RpcStatus> createIndex(CreateIndexParam requestParam) {
        return retry(() -> super.createIndex(requestParam));
    }

    @Override
    public R<RpcStatus> dropIndex(DropIndexParam requestParam) {
        return retry(() -> super.dropIndex(requestParam));
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam) {
        return retry(() -> super.describeIndex(requestParam));
    }

    @Override
    public R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam) {
        ExceptionUtils.checkNotNull(requestParam, requestParam.getClass().getSimpleName());
        return retry(() -> super.getIndexState(requestParam));
    }

    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam) {
        ExceptionUtils.checkNotNull(requestParam, requestParam.getClass().getSimpleName());
        return retry(() -> super.getIndexBuildProgress(requestParam));
    }

    @Override
    public R<MutationResult> insert(InsertParam requestParam) {
        return retry(() -> super.insert(requestParam));
    }

    @Override
    public R<MutationResult> upsert(UpsertParam requestParam) {
        return retry(() -> super.upsert(requestParam));
    }

    @Override
    public R<MutationResult> delete(DeleteParam requestParam) {
        return retry(() -> super.delete(requestParam));
    }

    @Override
    public R<SearchResults> search(SearchParam requestParam) {
        return retry(() -> super.search(requestParam));
    }

    @Override
    public R<SearchResults> hybridSearch(HybridSearchParam requestParam) {
        return retry(() -> super.hybridSearch(requestParam));
    }

    @Override
    public R<QueryResults> query(QueryParam requestParam) {
        return retry(() -> super.query(requestParam));
    }

    @Override
    public R<GetMetricsResponse> getMetrics(GetMetricsParam requestParam) {
        return retry(() -> super.getMetrics(requestParam));
    }

    @Override
    public R<GetFlushStateResponse> getFlushState(GetFlushStateParam requestParam) {
        return retry(() -> super.getFlushState(requestParam));
    }

    @Override
    public R<GetFlushAllStateResponse> getFlushAllState(GetFlushAllStateParam requestParam) {
        return retry(() -> super.getFlushAllState(requestParam));
    }

    @Override
    public R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(GetPersistentSegmentInfoParam requestParam) {
        return retry(() -> super.getPersistentSegmentInfo(requestParam));
    }

    @Override
    public R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(GetQuerySegmentInfoParam requestParam) {
        return retry(() -> super.getQuerySegmentInfo(requestParam));
    }

    @Override
    public R<GetReplicasResponse> getReplicas(GetReplicasParam requestParam) {
        return retry(() -> super.getReplicas(requestParam));
    }

    @Override
    public R<RpcStatus> loadBalance(LoadBalanceParam requestParam) {
        return retry(() -> super.loadBalance(requestParam));
    }

    @Override
    public R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam) {
        return retry(() -> super.getCompactionState(requestParam));
    }

    @Override
    public R<ManualCompactionResponse> manualCompact(ManualCompactParam requestParam) {
        return retry(() -> super.manualCompact(requestParam));
    }

    @Override
    public R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam) {
        return retry(() -> super.getCompactionStateWithPlans(requestParam));
    }

    @Override
    public R<RpcStatus> createCredential(CreateCredentialParam requestParam) {
        return retry(() -> super.createCredential(requestParam));
    }

    @Override
    public R<RpcStatus> updateCredential(UpdateCredentialParam requestParam) {
        return retry(() -> super.updateCredential(requestParam));
    }

    @Override
    public R<RpcStatus> deleteCredential(DeleteCredentialParam requestParam) {
        return retry(() -> super.deleteCredential(requestParam));
    }

    @Override
    public R<ListCredUsersResponse> listCredUsers(ListCredUsersParam requestParam) {
        return retry(() -> super.listCredUsers(requestParam));
    }


    @Override
    public R<RpcStatus> createRole(CreateRoleParam requestParam) {
        return retry(() -> super.createRole(requestParam));
    }


    @Override
    public R<RpcStatus> dropRole(DropRoleParam requestParam) {
        return retry(() -> super.dropRole(requestParam));
    }


    @Override
    public R<RpcStatus> addUserToRole(AddUserToRoleParam requestParam) {
        return retry(() -> super.addUserToRole(requestParam));
    }


    @Override
    public R<RpcStatus> removeUserFromRole(RemoveUserFromRoleParam requestParam) {
        return retry(() -> super.removeUserFromRole(requestParam));
    }


    @Override
    public R<SelectRoleResponse> selectRole(SelectRoleParam requestParam) {
        return retry(() -> super.selectRole(requestParam));
    }


    @Override
    public R<SelectUserResponse> selectUser(SelectUserParam requestParam) {
        return retry(() -> super.selectUser(requestParam));
    }


    @Override
    public R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam) {
        return retry(() -> super.grantRolePrivilege(requestParam));
    }


    @Override
    public R<RpcStatus> revokeRolePrivilege(RevokeRolePrivilegeParam requestParam) {
        return retry(() -> super.revokeRolePrivilege(requestParam));
    }


    @Override
    public R<SelectGrantResponse> selectGrantForRole(SelectGrantForRoleParam requestParam) {
        return retry(() -> super.selectGrantForRole(requestParam));
    }


    @Override
    public R<SelectGrantResponse> selectGrantForRoleAndObject(SelectGrantForRoleAndObjectParam requestParam) {
        return retry(() -> super.selectGrantForRoleAndObject(requestParam));
    }

    @Override
    public R<ImportResponse> bulkInsert(BulkInsertParam requestParam) {
        return retry(() -> super.bulkInsert(requestParam));
    }

    @Override
    public R<GetImportStateResponse> getBulkInsertState(GetBulkInsertStateParam requestParam) {
        return retry(() -> super.getBulkInsertState(requestParam));
    }

    @Override
    public R<ListImportTasksResponse> listBulkInsertTasks(ListBulkInsertTasksParam requestParam) {
        return retry(() -> super.listBulkInsertTasks(requestParam));
    }

    @Override
    public R<CheckHealthResponse> checkHealth() {
        return retry(super::checkHealth);
    }

    @Override
    public R<GetVersionResponse> getVersion() {
        return retry(super::getVersion);
    }

    @Override
    public R<GetLoadingProgressResponse> getLoadingProgress(GetLoadingProgressParam requestParam) {
        return retry(() -> super.getLoadingProgress(requestParam));
    }

    @Override
    public R<GetLoadStateResponse> getLoadState(GetLoadStateParam requestParam) {
        return retry(() -> super.getLoadState(requestParam));
    }

    @Override
    public R<RpcStatus> createResourceGroup(CreateResourceGroupParam requestParam) {
        return retry(() -> super.createResourceGroup(requestParam));
    }

    @Override
    public R<RpcStatus> dropResourceGroup(DropResourceGroupParam requestParam) {
        return retry(() -> super.dropResourceGroup(requestParam));
    }

    @Override
    public R<ListResourceGroupsResponse> listResourceGroups(ListResourceGroupsParam requestParam) {
        return retry(() -> super.listResourceGroups(requestParam));
    }

    @Override
    public R<DescribeResourceGroupResponse> describeResourceGroup(DescribeResourceGroupParam requestParam) {
        return retry(() -> super.describeResourceGroup(requestParam));
    }

    @Override
    public R<RpcStatus> transferNode(TransferNodeParam requestParam) {
        return retry(() -> super.transferNode(requestParam));
    }

    @Override
    public R<RpcStatus> transferReplica(TransferReplicaParam requestParam) {
        return retry(() -> super.transferReplica(requestParam));
    }

    @Override
    public R<RpcStatus> updateResourceGroups(UpdateResourceGroupsParam requestParam) {
        return retry(() -> super.updateResourceGroups(requestParam));
    }

    @Override
    public R<RpcStatus> createCollection(CreateSimpleCollectionParam requestParam) {
        return retry(() -> super.createCollection(requestParam));
    }

    @Override
    public R<ListCollectionsResponse> listCollections(ListCollectionsParam requestParam) {
        return retry(() -> super.listCollections(requestParam));
    }

    @Override
    public R<InsertResponse> insert(InsertRowsParam requestParam) {
        return retry(() -> super.insert(requestParam));
    }

    @Override
    public R<DeleteResponse> delete(DeleteIdsParam requestParam) {
        return retry(() -> super.delete(requestParam));
    }

    @Override
    public R<GetResponse> get(GetIdsParam requestParam) {
        return retry(() -> super.get(requestParam));
    }

    @Override
    public R<QueryResponse> query(QuerySimpleParam requestParam) {
        return retry(() -> super.query(requestParam));
    }

    @Override
    public R<SearchResponse> search(SearchSimpleParam requestParam) {
        return retry(() -> super.search(requestParam));
    }

    @Override
    public R<QueryIterator> queryIterator(QueryIteratorParam requestParam) {
        return retry(() -> super.queryIterator(requestParam));
    }

    @Override
    public R<SearchIterator> searchIterator(SearchIteratorParam requestParam) {
        return retry(() -> super.searchIterator(requestParam));
    }
}

