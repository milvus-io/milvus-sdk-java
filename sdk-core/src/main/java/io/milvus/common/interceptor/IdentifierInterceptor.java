package io.milvus.common.interceptor;

import io.grpc.*;

/**
 * A gRPC client interceptor that injects the server-assigned connection identifier
 * into every outgoing RPC request's metadata.
 *
 * <p>The identifier is obtained from {@code ConnectResponse.getIdentifier()} during
 * the initial Connect RPC, and is used by the server (especially kite-coordinator)
 * to associate subsequent requests with the connection-level state (e.g. cluster_id
 * binding for resource group routing).
 *
 * <p>This aligns Java SDK behavior with pymilvus, which implements the same mechanism
 * via {@code header_adder_interceptor}.
 */
public class IdentifierInterceptor implements ClientInterceptor {
    private static final Metadata.Key<String> IDENTIFIER_KEY =
            Metadata.Key.of("identifier", Metadata.ASCII_STRING_MARSHALLER);

    private final String identifier;

    public IdentifierInterceptor(long identifier) {
        this.identifier = String.valueOf(identifier);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(IDENTIFIER_KEY, identifier);
                super.start(responseListener, headers);
            }
        };
    }
}
