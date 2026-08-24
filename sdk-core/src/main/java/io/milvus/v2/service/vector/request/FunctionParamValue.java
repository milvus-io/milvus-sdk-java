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

package io.milvus.v2.service.vector.request;

import com.google.protobuf.ByteString;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.util.List;
import java.util.Map;

/**
 * A typed value used as a function-chain parameter or literal argument.
 *
 * <p>Values are converted from ordinary Java objects into the protobuf
 * {@code FunctionParamValue} message: {@code Boolean} -&gt; bool, integral numbers
 * ({@code Integer}/{@code Long}/{@code Short}/{@code Byte}/{@code BigInteger}, range-checked) -&gt;
 * int64, floating-point numbers -&gt; double, {@code String}/{@code Character} -&gt; string,
 * {@code byte[]}/{@code char[]}/{@link ByteString} -&gt; bytes, {@code List} -&gt; array,
 * {@code Map} -&gt; object.
 */
public class FunctionParamValue {
    private final io.milvus.grpc.FunctionParamValue grpcValue;

    private FunctionParamValue(io.milvus.grpc.FunctionParamValue grpcValue) {
        this.grpcValue = grpcValue;
    }

    public io.milvus.grpc.FunctionParamValue toGrpc() {
        return grpcValue;
    }

    public static FunctionParamValue of(boolean value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setBoolValue(value).build());
    }

    public static FunctionParamValue of(long value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setInt64Value(value).build());
    }

    public static FunctionParamValue of(double value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setDoubleValue(value).build());
    }

    public static FunctionParamValue of(String value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setStringValue(value).build());
    }

    public static FunctionParamValue of(byte[] value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setBytesValue(ByteString.copyFrom(value)).build());
    }

    public static FunctionParamValue of(ByteString value) {
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setBytesValue(value).build());
    }

    public static FunctionParamValue ofArray(List<FunctionParamValue> values) {
        io.milvus.grpc.FunctionParamArray.Builder builder = io.milvus.grpc.FunctionParamArray.newBuilder();
        values.forEach(v -> builder.addValues(v.toGrpc()));
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setArrayValue(builder.build()).build());
    }

    public static FunctionParamValue ofObject(Map<String, FunctionParamValue> fields) {
        io.milvus.grpc.FunctionParamObject.Builder builder = io.milvus.grpc.FunctionParamObject.newBuilder();
        fields.forEach((k, v) -> builder.putFields(k, v.toGrpc()));
        return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setObjectValue(builder.build()).build());
    }

    /**
     * Converts an arbitrary Java object into a {@link FunctionParamValue}.
     *
     * <p>Supported types mirror PyMilvus's {@code _to_param_value}: {@code Boolean},
     * {@code Integer}/{@code Long}/{@code Short}/{@code Byte}/{@code BigInteger} (range-checked to
     * int64), {@code Float}/{@code Double}, {@code String}/{@code Character},
     * {@code byte[]}/{@code char[]}/{@link ByteString}, {@code List} (recursively), and
     * {@code Map} (recursively).
     */
    public static FunctionParamValue from(Object value) {
        if (value == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function chain parameters do not support null");
        }
        if (value instanceof FunctionParamValue) {
            return (FunctionParamValue) value;
        }
        if (value instanceof Boolean) {
            return of((Boolean) value);
        }
        if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
            return of(((Number) value).longValue());
        }
        if (value instanceof java.math.BigInteger) {
            java.math.BigInteger bigInteger = (java.math.BigInteger) value;
            if (bigInteger.bitLength() > 63) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain integer parameter is out of int64 range: " + bigInteger);
            }
            return of(bigInteger.longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return of(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return of((String) value);
        }
        if (value instanceof Character) {
            return of(String.valueOf((Character) value));
        }
        if (value instanceof byte[]) {
            return of((byte[]) value);
        }
        if (value instanceof char[]) {
            return of(new String((char[]) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        if (value instanceof ByteString) {
            return of((ByteString) value);
        }
        if (value instanceof List) {
            io.milvus.grpc.FunctionParamArray.Builder builder = io.milvus.grpc.FunctionParamArray.newBuilder();
            for (Object item : (List<?>) value) {
                builder.addValues(from(item).toGrpc());
            }
            return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setArrayValue(builder.build()).build());
        }
        if (value instanceof Map) {
            io.milvus.grpc.FunctionParamObject.Builder builder = io.milvus.grpc.FunctionParamObject.newBuilder();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String) || ((String) entry.getKey()).isEmpty()) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Function chain parameter names must be non-empty strings");
                }
                builder.putFields((String) entry.getKey(), from(entry.getValue()).toGrpc());
            }
            return wrap(io.milvus.grpc.FunctionParamValue.newBuilder().setObjectValue(builder.build()).build());
        }
        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                "Unsupported function chain parameter type: " + value.getClass().getName());
    }

    private static FunctionParamValue wrap(io.milvus.grpc.FunctionParamValue grpcValue) {
        return new FunctionParamValue(grpcValue);
    }
}
