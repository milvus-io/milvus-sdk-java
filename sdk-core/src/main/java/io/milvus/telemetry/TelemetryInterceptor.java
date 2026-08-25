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

package io.milvus.telemetry;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.milvus.common.interceptor.ClientRequestInterceptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Records operation telemetry at the common unary gRPC boundary. */
public final class TelemetryInterceptor implements ClientInterceptor {
    private static final Set<String> OPERATIONS = new HashSet<>(Arrays.asList(
            "Insert", "Delete", "Upsert", "Search", "HybridSearch", "Query", "RunAnalyzer"));
    public static final CallOptions.Key<Boolean> LOGICAL_OPERATION_OPTION =
            CallOptions.Key.create("milvus-telemetry-logical-operation");

    private final ClientTelemetryManager manager;
    private final ThreadLocal<String> requestId;

    public TelemetryInterceptor(ClientTelemetryManager manager, ThreadLocal<String> requestId) {
        this.manager = manager;
        this.requestId = requestId;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        String operation = method.getBareMethodName();
        if (!OPERATIONS.contains(operation)) {
            return next.newCall(method, callOptions);
        }
        if (Boolean.TRUE.equals(callOptions.getOption(LOGICAL_OPERATION_OPTION))
                || manager.isLogicalOperationActive()) {
            return next.newCall(method, callOptions);
        }
        long startNanos = System.nanoTime();
        String currentRequestId = requestId(callOptions, requestId);
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {
            private String collection = "";
            private boolean businessSuccess = true;
            private String businessError = "";

            @Override
            public void sendMessage(ReqT message) {
                collection = collectionName(message);
                super.sendMessage(message);
            }

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                Listener<RespT> listener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onMessage(RespT message) {
                        businessSuccess = responseSucceeded(message);
                        if (!businessSuccess) {
                            businessError = responseError(message);
                        }
                        super.onMessage(message);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        String error = status.isOk() && businessSuccess
                                ? ""
                                : (!status.isOk() ? status.toString() : businessError);
                        try {
                            manager.recordOperation(
                                    operation, collection, startNanos, error, currentRequestId);
                        } catch (RuntimeException ignored) {
                            // Telemetry is best-effort and must not suppress the business callback.
                        }
                        super.onClose(status, trailers);
                    }
                };
                super.start(listener, headers);
            }
        };
    }

    static String requestId(CallOptions callOptions, ThreadLocal<String> fallback) {
        String value = callOptions.getOption(ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION);
        if (value == null && fallback != null) {
            value = fallback.get();
        }
        return ClientRequestInterceptor.isValidClientRequestId(value) ? value : "";
    }

    private static String collectionName(Object request) {
        if (!(request instanceof Message)) {
            return "";
        }
        Message message = (Message) request;
        Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName("collection_name");
        if (field == null) {
            return "";
        }
        return String.valueOf(message.getField(field));
    }

    private static boolean responseSucceeded(Object response) {
        Message status = statusMessage(response);
        if (status == null) {
            return true;
        }
        Descriptors.FieldDescriptor code = status.getDescriptorForType().findFieldByName("code");
        Descriptors.FieldDescriptor errorCode = status.getDescriptorForType().findFieldByName("error_code");
        return numericValue(status, code) == 0 && numericValue(status, errorCode) == 0;
    }

    private static String responseError(Object response) {
        Message status = statusMessage(response);
        if (status == null) {
            return "Milvus operation failed";
        }
        Descriptors.FieldDescriptor reason = status.getDescriptorForType().findFieldByName("reason");
        return reason == null ? "Milvus operation failed" : String.valueOf(status.getField(reason));
    }

    private static Message statusMessage(Object response) {
        if (!(response instanceof Message)) {
            return null;
        }
        Message message = (Message) response;
        Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName("status");
        if (field == null || !message.hasField(field)) {
            return null;
        }
        Object value = message.getField(field);
        return value instanceof Message ? (Message) value : null;
    }

    private static int numericValue(Message message, Descriptors.FieldDescriptor field) {
        if (field == null) {
            return 0;
        }
        Object value = message.getField(field);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof Descriptors.EnumValueDescriptor) {
            return ((Descriptors.EnumValueDescriptor) value).getNumber();
        }
        return 0;
    }
}
