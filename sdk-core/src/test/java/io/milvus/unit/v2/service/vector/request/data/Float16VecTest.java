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

package io.milvus.unit.v2.service.vector.request.data;

import io.milvus.grpc.PlaceholderType;
import io.milvus.v2.service.vector.request.data.Float16Vec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class Float16VecTest {

    @Test
    void byteBufferConstructorPreservesBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        Float16Vec vec = new Float16Vec(buffer);

        assertSame(buffer, vec.getData());
        assertEquals(4, ((ByteBuffer) vec.getData()).limit());
    }

    @Test
    void byteArrayConstructorWrapsIntoBuffer() {
        Float16Vec vec = new Float16Vec(new byte[]{1, 2, 3, 4});

        ByteBuffer data = (ByteBuffer) vec.getData();
        assertEquals(4, data.limit());
        assertEquals(1, data.get(0));
        assertEquals(4, data.get(3));
    }

    @Test
    void floatListConstructorEncodesTwoBytesPerDim() {
        List<Float> floats = Arrays.asList(1.0f, 2.0f, -1.5f);
        Float16Vec vec = new Float16Vec(floats);

        ByteBuffer data = (ByteBuffer) vec.getData();
        assertEquals(2 * floats.size(), data.limit());
        assertEquals(2 * floats.size(), data.capacity());
    }

    @Test
    void placeholderTypeIsFloat16Vector() {
        assertEquals(PlaceholderType.Float16Vector,
                new Float16Vec(ByteBuffer.wrap(new byte[]{0, 0})).getPlaceholderType());
    }
}
