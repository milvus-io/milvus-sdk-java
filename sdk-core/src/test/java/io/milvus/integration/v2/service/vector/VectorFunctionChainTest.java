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

package io.milvus.integration.v2.service.vector;

import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainOp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Tag("integration")
class VectorFunctionChainTest extends BaseTest {

    @Test
    void testFunctionParamValueFrom() {
        // scalar types
        io.milvus.grpc.FunctionParamValue boolValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(true).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.BOOL_VALUE, boolValue.getValueCase());
        Assertions.assertTrue(boolValue.getBoolValue());

        io.milvus.grpc.FunctionParamValue intValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(42).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.INT64_VALUE, intValue.getValueCase());
        Assertions.assertEquals(42L, intValue.getInt64Value());

        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from(42L).toGrpc().getInt64Value());
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from((short) 42).toGrpc().getInt64Value());
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from((byte) 42).toGrpc().getInt64Value());

        io.milvus.grpc.FunctionParamValue doubleValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(0.5).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.DOUBLE_VALUE, doubleValue.getValueCase());
        Assertions.assertEquals(0.5, doubleValue.getDoubleValue());
        Assertions.assertEquals(0.5, io.milvus.v2.service.vector.request.FunctionParamValue.from(0.5f).toGrpc().getDoubleValue());

        io.milvus.grpc.FunctionParamValue stringValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from("weighted").toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.STRING_VALUE, stringValue.getValueCase());
        Assertions.assertEquals("weighted", stringValue.getStringValue());

        byte[] bytes = new byte[]{1, 2, 3};
        io.milvus.grpc.FunctionParamValue bytesValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(bytes).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.BYTES_VALUE, bytesValue.getValueCase());
        Assertions.assertArrayEquals(bytes, bytesValue.getBytesValue().toByteArray());

        // container types, recursively converted
        io.milvus.grpc.FunctionParamValue arrayValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(Arrays.asList(1, 2.0, "three")).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.ARRAY_VALUE, arrayValue.getValueCase());
        Assertions.assertEquals(3, arrayValue.getArrayValue().getValuesCount());
        Assertions.assertEquals(1L, arrayValue.getArrayValue().getValues(0).getInt64Value());
        Assertions.assertEquals(2.0, arrayValue.getArrayValue().getValues(1).getDoubleValue());
        Assertions.assertEquals("three", arrayValue.getArrayValue().getValues(2).getStringValue());

        Map<String, Object> objectInput = new LinkedHashMap<>();
        objectInput.put("mode", "sum");
        objectInput.put("weights", Arrays.asList(0.7, 0.3));
        io.milvus.grpc.FunctionParamValue objectValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(objectInput).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.OBJECT_VALUE, objectValue.getValueCase());
        Assertions.assertEquals("sum", objectValue.getObjectValue().getFieldsOrThrow("mode").getStringValue());
        Assertions.assertEquals(2, objectValue.getObjectValue().getFieldsOrThrow("weights").getArrayValue().getValuesCount());

        // a FunctionParamValue passes through unchanged
        io.milvus.v2.service.vector.request.FunctionParamValue passthrough =
                io.milvus.v2.service.vector.request.FunctionParamValue.of(7L);
        Assertions.assertSame(passthrough, io.milvus.v2.service.vector.request.FunctionParamValue.from(passthrough));

        // error paths
        MilvusClientException nullEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullEx.getErrorCode());

        MilvusClientException unsupportedEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(new Object()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, unsupportedEx.getErrorCode());

        Map<Object, Object> badKeyInput = new HashMap<>();
        badKeyInput.put(1, "x");
        MilvusClientException badKeyEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(badKeyInput));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, badKeyEx.getErrorCode());
    }

    @Test
    void testFunctionParamValueFromAdditionalTypes() {
        // BigInteger within int64 range maps to int64
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue
                .from(java.math.BigInteger.valueOf(42)).toGrpc().getInt64Value());
        // BigInteger out of int64 range is rejected with a clear error
        MilvusClientException overflowEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue
                        .from(java.math.BigInteger.valueOf(Long.MAX_VALUE).add(java.math.BigInteger.ONE)));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, overflowEx.getErrorCode());

        // Character maps to a one-char string
        Assertions.assertEquals("x", io.milvus.v2.service.vector.request.FunctionParamValue
                .from('x').toGrpc().getStringValue());

        // char[] maps to UTF-8 bytes
        Assertions.assertArrayEquals("hi".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                io.milvus.v2.service.vector.request.FunctionParamValue
                        .from(new char[]{'h', 'i'}).toGrpc().getBytesValue().toByteArray());
    }

    @Test
    void testFunctionChainFactoryValidation() {
        // map: empty output
        MilvusClientException emptyMapOutput = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map("", FunctionChainExpr.builder().name("f").build()).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, emptyMapOutput.getErrorCode());

        // map: null expr
        MilvusClientException nullMapExpr = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map("$score", null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullMapExpr.getErrorCode());

        // sort: empty by
        MilvusClientException emptySortBy = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().sort("", true, null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, emptySortBy.getErrorCode());

        // limit: non-positive limit
        MilvusClientException nonPositiveLimit = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(0).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nonPositiveLimit.getErrorCode());

        // limit: negative offset
        MilvusClientException negativeOffset = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(10, -1).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, negativeOffset.getErrorCode());
    }

    @Test
    void testFunctionChainBuilderNullGuards() {
        // null stage is rejected eagerly instead of failing at serialization
        MilvusClientException nullStage = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().stage(null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullStage.getErrorCode());

        // null op params are rejected with a typed error
        MilvusClientException nullParams = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChainOp.builder().op("map").params(null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullParams.getErrorCode());
    }
}
