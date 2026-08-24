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
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientRequestInterceptorTest {
    private static final String CALL_OPTION_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String THREAD_LOCAL_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final Metadata.Key<String> CLIENT_REQUEST_ID_HEADER =
            Metadata.Key.of("client_request_id", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    void explicitRequestIdOverridesExecutingThread() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set(THREAD_LOCAL_ID);

        String captured = intercept(requestId, CallOptions.DEFAULT.withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, CALL_OPTION_ID));

        assertEquals(CALL_OPTION_ID, captured);
    }

    @Test
    void explicitEmptyRequestIdSuppressesExecutingThread() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set(THREAD_LOCAL_ID);

        String captured = intercept(requestId, CallOptions.DEFAULT.withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, ""));

        assertNull(captured);
    }

    @Test
    void fallsBackToThreadLocalWithoutExplicitRequestId() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set(THREAD_LOCAL_ID);

        assertEquals(THREAD_LOCAL_ID, intercept(requestId, CallOptions.DEFAULT));
    }

    @Test
    void preservesArbitraryAndUppercaseWireCorrelationIds() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set(THREAD_LOCAL_ID);

        assertEquals("query-request", intercept(requestId, CallOptions.DEFAULT.withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "query-request")));
        assertEquals("4BF92F3577B34DA6A3CE929D0E0E4736",
                intercept(requestId, CallOptions.DEFAULT.withOption(
                        ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                        "4BF92F3577B34DA6A3CE929D0E0E4736")));
        requestId.set("not-a-trace-id");
        assertEquals("not-a-trace-id", intercept(requestId, CallOptions.DEFAULT));
    }

    @Test
    void validatesNonZeroLowercaseTraceIds() {
        assertTrue(ClientRequestInterceptor.isValidClientRequestId(CALL_OPTION_ID));
        assertFalse(ClientRequestInterceptor.isValidClientRequestId("00000000000000000000000000000000"));
        assertFalse(ClientRequestInterceptor.isValidClientRequestId("4BF92F3577B34DA6A3CE929D0E0E4736"));
        assertFalse(ClientRequestInterceptor.isValidClientRequestId("4bf92f3577b34da6a3ce929d0e0e473g"));
        assertFalse(ClientRequestInterceptor.isValidClientRequestId("4bf92f35"));
    }

    private String intercept(ThreadLocal<String> requestId, CallOptions callOptions) {
        String[] captured = {null};
        ClientRequestInterceptor interceptor = new ClientRequestInterceptor(requestId);
        ClientCall<Object, Object> call = interceptor.interceptCall(
                method(), callOptions, channel(captured));
        call.start(new ClientCall.Listener<Object>() {}, new Metadata());
        return captured[0];
    }

    private Channel channel(String[] captured) {
        return new Channel() {
            @Override
            public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                    MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<RequestT, ResponseT>() {
                    @Override
                    public void start(Listener<ResponseT> responseListener, Metadata headers) {
                        captured[0] = headers.get(CLIENT_REQUEST_ID_HEADER);
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void halfClose() {}

                    @Override
                    public void sendMessage(RequestT message) {}
                };
            }

            @Override
            public String authority() {
                return "test";
            }
        };
    }

    private MethodDescriptor<Object, Object> method() {
        return MethodDescriptor.newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test/method")
                .setRequestMarshaller(new NoopMarshaller())
                .setResponseMarshaller(new NoopMarshaller())
                .build();
    }

    private static class NoopMarshaller implements MethodDescriptor.Marshaller<Object> {
        @Override
        public InputStream stream(Object value) {
            return null;
        }

        @Override
        public Object parse(InputStream stream) {
            return null;
        }
    }
}
