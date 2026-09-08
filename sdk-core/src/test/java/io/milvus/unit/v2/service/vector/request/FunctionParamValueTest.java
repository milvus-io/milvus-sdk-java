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

package io.milvus.unit.v2.service.vector.request;

import com.google.protobuf.ByteString;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.FunctionParamValue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionParamValueTest {

    @Test
    void primitiveFactoriesSetGrpcFields() {
        assertEquals(true, FunctionParamValue.of(true).toGrpc().getBoolValue());
        assertEquals(42L, FunctionParamValue.of(42L).toGrpc().getInt64Value());
        assertEquals(0.5d, FunctionParamValue.of(0.5d).toGrpc().getDoubleValue());
        assertEquals("hello", FunctionParamValue.of("hello").toGrpc().getStringValue());
        assertEquals("by", FunctionParamValue.of(new byte[]{'b', 'y'}).toGrpc().getBytesValue().toStringUtf8());
        assertEquals("ab", FunctionParamValue.of(ByteString.copyFromUtf8("ab")).toGrpc().getBytesValue().toStringUtf8());
    }

    @Test
    void ofArrayAndObject() {
        FunctionParamValue array = FunctionParamValue.ofArray(
                Arrays.asList(FunctionParamValue.of(1L), FunctionParamValue.of("x")));
        assertEquals(2, array.toGrpc().getArrayValue().getValuesCount());

        Map<String, FunctionParamValue> fields = new HashMap<>();
        fields.put("k", FunctionParamValue.of(true));
        FunctionParamValue object = FunctionParamValue.ofObject(fields);
        assertEquals(1, object.toGrpc().getObjectValue().getFieldsCount());
        assertTrue(object.toGrpc().getObjectValue().getFieldsMap().containsKey("k"));
    }

    @Test
    void fromConvertsScalarTypes() {
        assertEquals(true, FunctionParamValue.from(true).toGrpc().getBoolValue());
        assertEquals(1L, FunctionParamValue.from(1).toGrpc().getInt64Value());
        assertEquals(2L, FunctionParamValue.from(2L).toGrpc().getInt64Value());
        assertEquals(3L, FunctionParamValue.from((short) 3).toGrpc().getInt64Value());
        assertEquals(4L, FunctionParamValue.from((byte) 4).toGrpc().getInt64Value());
        assertEquals(5L, FunctionParamValue.from(BigInteger.valueOf(5)).toGrpc().getInt64Value());
        assertEquals(6.5d, FunctionParamValue.from(6.5f).toGrpc().getDoubleValue());
        assertEquals(7.5d, FunctionParamValue.from(7.5d).toGrpc().getDoubleValue());
        assertEquals("str", FunctionParamValue.from("str").toGrpc().getStringValue());
        assertEquals("c", FunctionParamValue.from('c').toGrpc().getStringValue());
        assertEquals("ab", FunctionParamValue.from(new byte[]{'a', 'b'}).toGrpc().getBytesValue().toStringUtf8());
        assertEquals("cd", FunctionParamValue.from(new char[]{'c', 'd'}).toGrpc().getBytesValue().toStringUtf8());
        assertEquals("ef", FunctionParamValue.from(ByteString.copyFromUtf8("ef")).toGrpc().getBytesValue().toStringUtf8());
    }

    @Test
    void fromPassthroughReturnsSameInstance() {
        FunctionParamValue value = FunctionParamValue.of("x");
        assertSame(value, FunctionParamValue.from(value));
    }

    @Test
    void fromBigIntegerRangeChecked() {
        assertThrows(MilvusClientException.class,
                () -> FunctionParamValue.from(BigInteger.ONE.shiftLeft(63)));
        assertEquals(Long.MAX_VALUE,
                FunctionParamValue.from(BigInteger.valueOf(Long.MAX_VALUE)).toGrpc().getInt64Value());
        assertEquals(Long.MIN_VALUE,
                FunctionParamValue.from(BigInteger.valueOf(Long.MIN_VALUE)).toGrpc().getInt64Value());
    }

    @Test
    void fromRecursesIntoListAndMap() {
        List<Object> list = Arrays.asList(1, "two", true);
        assertEquals(3, FunctionParamValue.from(list).toGrpc().getArrayValue().getValuesCount());

        Map<String, Object> map = new HashMap<>();
        map.put("nested", Arrays.asList(1, 2));
        map.put("flag", true);
        io.milvus.grpc.FunctionParamValue grpc = FunctionParamValue.from(map).toGrpc();
        assertEquals(2, grpc.getObjectValue().getFieldsCount());
        assertEquals(2, grpc.getObjectValue().getFieldsMap().get("nested").getArrayValue().getValuesCount());
    }

    @Test
    void fromRejectsInvalidInputs() {
        assertThrows(MilvusClientException.class, () -> FunctionParamValue.from(null));
        assertThrows(MilvusClientException.class, () -> FunctionParamValue.from(new Object()));

        Map<String, Object> badKey = new HashMap<>();
        badKey.put("", 1);
        assertThrows(MilvusClientException.class, () -> FunctionParamValue.from(badKey));

        Map<Object, Object> nonStringKey = new HashMap<>();
        nonStringKey.put(1, "x");
        assertThrows(MilvusClientException.class, () -> FunctionParamValue.from(nonStringKey));
    }
}
