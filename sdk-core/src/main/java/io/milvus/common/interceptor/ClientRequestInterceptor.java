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

package io.milvus.common.interceptor;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.commons.lang3.StringUtils;

public class ClientRequestInterceptor implements ClientInterceptor {
    public static final CallOptions.Key<String> CLIENT_REQUEST_ID_OPTION =
            CallOptions.Key.create("milvus-client-request-id");

    private static final Metadata.Key<String> CLIENT_REQUEST_ID_HEADER =
            Metadata.Key.of("client_request_id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CLIENT_REQUEST_UNIX_MSEC_HEADER =
            Metadata.Key.of("client-request-unixmsec", Metadata.ASCII_STRING_MARSHALLER);

    private final ThreadLocal<String> clientRequestId;

    public ClientRequestInterceptor(ThreadLocal<String> clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        String capturedRequestId = callOptions.getOption(CLIENT_REQUEST_ID_OPTION);
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(CLIENT_REQUEST_UNIX_MSEC_HEADER, String.valueOf(System.currentTimeMillis()));
                String requestId = capturedRequestId;
                if (requestId == null && clientRequestId != null) {
                    requestId = clientRequestId.get();
                }
                // client_request_id predates telemetry and accepts arbitrary caller-provided
                // correlation IDs. Telemetry separately applies the strict OTel trace-ID check
                // before copying this value into error details.
                if (StringUtils.isNotEmpty(requestId)) {
                    headers.put(CLIENT_REQUEST_ID_HEADER, requestId);
                }
                super.start(responseListener, headers);
            }
        };
    }

    /** Mirrors the server's OpenTelemetry TraceID parser. */
    public static boolean isValidClientRequestId(String requestId) {
        if (StringUtils.length(requestId) != 32) {
            return false;
        }
        boolean nonZero = false;
        for (int index = 0; index < requestId.length(); index++) {
            char value = requestId.charAt(index);
            if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f'))) {
                return false;
            }
            nonZero |= value != '0';
        }
        return nonZero;
    }
}
