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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResults;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryInterceptorTest {
    @Test
    void recordsBusinessAndTransportFailuresAtUnaryBoundary() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null);
        CapturingChannel channel = new CapturingChannel();
        TelemetryInterceptor interceptor = new TelemetryInterceptor(manager, null);
        try {
            ClientCall<SearchRequest, SearchResults> businessCall = interceptor.interceptCall(
                    MilvusServiceGrpc.getSearchMethod(), CallOptions.DEFAULT, channel);
            businessCall.start(new ClientCall.Listener<SearchResults>() {}, new Metadata());
            businessCall.sendMessage(SearchRequest.newBuilder().setCollectionName("books").build());
            channel.listener.onMessage(SearchResults.newBuilder()
                    .setStatus(io.milvus.grpc.Status.newBuilder()
                            .setCode(1)
                            .setReason("business failure")
                            .build())
                    .build());
            channel.listener.onClose(Status.OK, new Metadata());

            ClientCall<SearchRequest, SearchResults> transportCall = interceptor.interceptCall(
                    MilvusServiceGrpc.getSearchMethod(), CallOptions.DEFAULT, channel);
            transportCall.start(new ClientCall.Listener<SearchResults>() {}, new Metadata());
            transportCall.sendMessage(SearchRequest.newBuilder().setCollectionName("books").build());
            channel.listener.onClose(Status.UNAVAILABLE.withDescription("offline"), new Metadata());

            assertEquals(2, manager.getRecentErrors(10).size());
            assertEquals("business failure", manager.getRecentErrors(10).get(1).error_msg);
            assertEquals("books", manager.getRecentErrors(10).get(1).collection);
            assertTrue(manager.getRecentErrors(10).get(0).error_msg.contains("UNAVAILABLE"));
        } finally {
            manager.close();
        }
    }

    @Test
    void bypassesNonOperationsExplicitLogicalCallsAndNestedScopes() {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null);
        CapturingChannel channel = new CapturingChannel();
        TelemetryInterceptor interceptor = new TelemetryInterceptor(manager, null);
        try {
            ClientCall<?, ?> nonOperation = interceptor.interceptCall(
                    MilvusServiceGrpc.getConnectMethod(), CallOptions.DEFAULT, channel);
            assertSame(channel.lastCall, nonOperation);

            ClientCall<?, ?> explicitLogical = interceptor.interceptCall(
                    MilvusServiceGrpc.getSearchMethod(),
                    CallOptions.DEFAULT.withOption(TelemetryInterceptor.LOGICAL_OPERATION_OPTION, true),
                    channel);
            assertSame(channel.lastCall, explicitLogical);

            TelemetryInterceptor.LogicalOperationScope outer =
                    TelemetryInterceptor.beginLogicalOperation();
            TelemetryInterceptor.LogicalOperationScope inner =
                    TelemetryInterceptor.beginLogicalOperation();
            ClientCall<?, ?> nested = interceptor.interceptCall(
                    MilvusServiceGrpc.getSearchMethod(), CallOptions.DEFAULT, channel);
            assertSame(channel.lastCall, nested);
            inner.close();
            outer.close();
            outer.close();

            ClientCall<?, ?> instrumented = interceptor.interceptCall(
                    MilvusServiceGrpc.getSearchMethod(), CallOptions.DEFAULT, channel);
            assertFalse(channel.lastCall == instrumented);
            assertEquals(4, channel.calls.get());
        } finally {
            manager.close();
        }
    }

    @Test
    void protobufExtractionFailsOpenForUnrelatedValues() throws Exception {
        assertEquals("", invokeStatic("collectionName", "not-protobuf"));
        assertEquals(true, invokeStatic("responseSucceeded", "not-protobuf"));
        assertEquals("Milvus operation failed", invokeStatic("responseError", "not-protobuf"));
        assertNull(invokeStatic("statusMessage", SearchRequest.getDefaultInstance()));

        SearchRequest request = SearchRequest.newBuilder().setCollectionName("books").build();
        assertEquals("books", invokeStatic("collectionName", request));
    }

    private static Object invokeStatic(String name, Object argument) throws Exception {
        Method method = TelemetryInterceptor.class.getDeclaredMethod(name, Object.class);
        method.setAccessible(true);
        return method.invoke(null, argument);
    }

    private static final class CapturingChannel extends Channel {
        private final AtomicInteger calls = new AtomicInteger();
        private ClientCall<?, ?> lastCall;
        private ClientCall.Listener listener;

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            calls.incrementAndGet();
            ClientCall<RequestT, ResponseT> call = new ClientCall<RequestT, ResponseT>() {
                @Override
                public void start(Listener<ResponseT> responseListener, Metadata headers) {
                    listener = responseListener;
                }

                @Override
                public void request(int numMessages) {
                }

                @Override
                public void cancel(String message, Throwable cause) {
                }

                @Override
                public void halfClose() {
                }

                @Override
                public void sendMessage(RequestT message) {
                }
            };
            lastCall = call;
            return call;
        }

        @Override
        public String authority() {
            return "test";
        }
    }
}
