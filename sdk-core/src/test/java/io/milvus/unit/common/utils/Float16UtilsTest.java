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

package io.milvus.unit.common.utils;

import io.milvus.common.utils.Float16Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tag("unit")
class Float16UtilsTest {

    @Test
    void fp16KnownBitPatterns() {
        Assertions.assertEquals((short) 0x0000, Float16Utils.floatToFp16(0.0f));
        Assertions.assertEquals((short) 0x8000, Float16Utils.floatToFp16(-0.0f));
        Assertions.assertEquals((short) 0x3C00, Float16Utils.floatToFp16(1.0f));
        Assertions.assertEquals((short) 0xBC00, Float16Utils.floatToFp16(-1.0f));
        Assertions.assertEquals((short) 0x4000, Float16Utils.floatToFp16(2.0f));
        Assertions.assertEquals((short) 0x7BFF, Float16Utils.floatToFp16(65504.0f));
        Assertions.assertEquals((short) 0x7C00, Float16Utils.floatToFp16(Float.POSITIVE_INFINITY));
        Assertions.assertEquals((short) 0xFC00, Float16Utils.floatToFp16(Float.NEGATIVE_INFINITY));
        Assertions.assertEquals((short) 0x7E00, Float16Utils.floatToFp16(Float.NaN));
    }

    @Test
    void fp16DecodeKnownBitPatterns() {
        Assertions.assertEquals(1.0f, Float16Utils.fp16ToFloat((short) 0x3C00));
        Assertions.assertEquals(-1.0f, Float16Utils.fp16ToFloat((short) 0xBC00));
        Assertions.assertEquals(2.0f, Float16Utils.fp16ToFloat((short) 0x4000));
        Assertions.assertEquals(-2.0f, Float16Utils.fp16ToFloat((short) 0xC000));
        Assertions.assertEquals(0.0f, Float16Utils.fp16ToFloat((short) 0x0000));
        Assertions.assertEquals(-0.0f, Float16Utils.fp16ToFloat((short) 0x8000));
        Assertions.assertEquals(5.9604645E-8f, Float16Utils.fp16ToFloat((short) 0x0001));
        Assertions.assertEquals(Float.POSITIVE_INFINITY, Float16Utils.fp16ToFloat((short) 0x7C00));
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, Float16Utils.fp16ToFloat((short) 0xFC00));
    }

    @Test
    void fp16RoundTripOnExactValues() {
        float[] values = {0.0f, -0.0f, 1.0f, -1.0f, 1.5f, -2.25f, 0.5f, 65504.0f, -65504.0f};
        for (float v : values) {
            Assertions.assertEquals(v, Float16Utils.fp16ToFloat(Float16Utils.floatToFp16(v)),
                    "fp16 round trip failed for " + v);
        }
    }

    @Test
    void fp16NaNFlagsStayNaN() {
        Assertions.assertTrue(Float.isNaN(Float16Utils.fp16ToFloat(Float16Utils.floatToFp16(Float.NaN))));
    }

    @Test
    void bf16KnownBitPatterns() {
        Assertions.assertEquals((short) 0x0000, Float16Utils.floatToBf16(0.0f));
        Assertions.assertEquals((short) 0x3F80, Float16Utils.floatToBf16(1.0f));
        Assertions.assertEquals((short) 0xBF80, Float16Utils.floatToBf16(-1.0f));
        Assertions.assertEquals((short) 0x3FC0, Float16Utils.floatToBf16(1.5f));
        Assertions.assertEquals((short) 0x7F80, Float16Utils.floatToBf16(Float.POSITIVE_INFINITY));
        Assertions.assertEquals((short) 0xFF80, Float16Utils.floatToBf16(Float.NEGATIVE_INFINITY));
    }

    @Test
    void bf16DecodeKnownBitPatterns() {
        Assertions.assertEquals(1.0f, Float16Utils.bf16ToFloat((short) 0x3F80));
        Assertions.assertEquals(-1.0f, Float16Utils.bf16ToFloat((short) 0xBF80));
        Assertions.assertEquals(1.5f, Float16Utils.bf16ToFloat((short) 0x3FC0));
        Assertions.assertEquals(Float.POSITIVE_INFINITY, Float16Utils.bf16ToFloat((short) 0x7F80));
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, Float16Utils.bf16ToFloat((short) 0xFF80));
    }

    @Test
    void bf16RoundTripOnExactValues() {
        float[] values = {0.0f, -0.0f, 1.0f, -1.0f, 1.5f, 2.0f, -3.25f, 100.0f, -100.0f};
        for (float v : values) {
            Assertions.assertEquals(v, Float16Utils.bf16ToFloat(Float16Utils.floatToBf16(v)),
                    "bf16 round trip failed for " + v);
        }
    }

    @Test
    void f32VectorToFp16BufferProducesLittleEndianShorts() {
        ByteBuffer buf = Float16Utils.f32VectorToFp16Buffer(Arrays.asList(1.0f, 2.0f));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(ByteOrder.LITTLE_ENDIAN, buf.order());
        Assertions.assertEquals(4, buf.capacity());
        Assertions.assertEquals((short) 0x3C00, buf.getShort(0));
        Assertions.assertEquals((short) 0x4000, buf.getShort(2));
        Assertions.assertEquals((byte) 0x00, buf.get(0));
        Assertions.assertEquals((byte) 0x3C, buf.get(1));
    }

    @Test
    void f32VectorToBf16BufferProducesShorts() {
        ByteBuffer buf = Float16Utils.f32VectorToBf16Buffer(Arrays.asList(1.0f, -2.0f));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(ByteOrder.LITTLE_ENDIAN, buf.order());
        Assertions.assertEquals(4, buf.capacity());
        Assertions.assertEquals((short) 0x3F80, buf.getShort(0));
        Assertions.assertEquals((short) 0xC000, buf.getShort(2));
    }

    @Test
    void fp16BufferToVectorRoundTrip() {
        List<Float> input = Arrays.asList(1.0f, 2.0f, -1.5f, 0.0f);
        ByteBuffer buf = Float16Utils.f32VectorToFp16Buffer(input);
        Assertions.assertEquals(input, Float16Utils.fp16BufferToVector(buf));
    }

    @Test
    void bf16BufferToVectorRoundTrip() {
        List<Float> input = Arrays.asList(1.0f, 2.0f, -1.5f, 100.0f);
        ByteBuffer buf = Float16Utils.f32VectorToBf16Buffer(input);
        Assertions.assertEquals(input, Float16Utils.bf16BufferToVector(buf));
    }

    @Test
    void f16VectorToBufferAndBackRoundTrip() {
        List<Short> input = Arrays.asList((short) 0x3C00, (short) 0xBC00, (short) 0x4000);
        ByteBuffer buf = Float16Utils.f16VectorToBuffer(input);
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(ByteOrder.LITTLE_ENDIAN, buf.order());
        Assertions.assertEquals(6, buf.capacity());
        Assertions.assertEquals(input, Float16Utils.bufferToF16Vector(buf));
    }

    @Test
    void decodeFunctionsRewindBuffer() {
        ByteBuffer buf = Float16Utils.f32VectorToFp16Buffer(Arrays.asList(1.0f, 2.0f));
        buf.position(4);
        // rewind must reset the read position so the whole vector is decoded
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f), Float16Utils.fp16BufferToVector(buf));
    }

    @Test
    void emptyVectorConversionsReturnNull() {
        Assertions.assertNull(Float16Utils.f32VectorToFp16Buffer(Collections.emptyList()));
        Assertions.assertNull(Float16Utils.f32VectorToBf16Buffer(Collections.emptyList()));
        Assertions.assertNull(Float16Utils.f16VectorToBuffer(Collections.emptyList()));
    }
}
