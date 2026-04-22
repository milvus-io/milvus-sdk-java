package io.milvus.common.interceptor;

import io.grpc.*;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class IdentifierInterceptorTest {

    private static final Metadata.Key<String> IDENTIFIER_KEY =
            Metadata.Key.of("identifier", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    void testIdentifierInjectedIntoMetadata() {
        long identifier = 465795948303613954L;
        IdentifierInterceptor interceptor = new IdentifierInterceptor(identifier);

        final String[] capturedValue = {null};

        Channel mockChannel = new Channel() {
            @Override
            public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                    MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<RequestT, ResponseT>() {
                    @Override
                    public void start(ClientCall.Listener<ResponseT> responseListener, Metadata headers) {
                        capturedValue[0] = headers.get(IDENTIFIER_KEY);
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

        MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test/method")
                .setRequestMarshaller(new NoopMarshaller())
                .setResponseMarshaller(new NoopMarshaller())
                .build();

        ClientCall<Object, Object> call = interceptor.interceptCall(method, CallOptions.DEFAULT, mockChannel);
        call.start(new ClientCall.Listener<Object>() {}, new Metadata());

        assertEquals("465795948303613954", capturedValue[0]);
    }

    @Test
    void testExistingHeadersPreserved() {
        IdentifierInterceptor interceptor = new IdentifierInterceptor(100L);

        Metadata.Key<String> customKey = Metadata.Key.of("custom-header", Metadata.ASCII_STRING_MARSHALLER);
        final String[] capturedIdentifier = {null};
        final String[] capturedCustom = {null};

        Channel mockChannel = new Channel() {
            @Override
            public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                    MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<RequestT, ResponseT>() {
                    @Override
                    public void start(ClientCall.Listener<ResponseT> responseListener, Metadata headers) {
                        capturedIdentifier[0] = headers.get(IDENTIFIER_KEY);
                        capturedCustom[0] = headers.get(customKey);
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

        MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test/method")
                .setRequestMarshaller(new NoopMarshaller())
                .setResponseMarshaller(new NoopMarshaller())
                .build();

        ClientCall<Object, Object> call = interceptor.interceptCall(method, CallOptions.DEFAULT, mockChannel);

        Metadata existingHeaders = new Metadata();
        existingHeaders.put(customKey, "my-value");
        call.start(new ClientCall.Listener<Object>() {}, existingHeaders);

        assertEquals("100", capturedIdentifier[0]);
        assertEquals("my-value", capturedCustom[0]);
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
