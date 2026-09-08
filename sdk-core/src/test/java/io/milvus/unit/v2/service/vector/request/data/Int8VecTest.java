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
import io.milvus.v2.service.vector.request.data.Int8Vec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class Int8VecTest {

    @Test
    void byteBufferConstructorPreservesBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3});
        Int8Vec vec = new Int8Vec(buffer);

        assertSame(buffer, vec.getData());
        assertEquals(3, ((ByteBuffer) vec.getData()).limit());
    }

    @Test
    void byteArrayConstructorWrapsIntoBuffer() {
        Int8Vec vec = new Int8Vec(new byte[]{-128, 0, 127});

        ByteBuffer data = (ByteBuffer) vec.getData();
        assertEquals(3, data.limit());
        assertEquals(-128, data.get(0));
        assertEquals(0, data.get(1));
        assertEquals(127, data.get(2));
    }

    @Test
    void placeholderTypeIsInt8Vector() {
        assertEquals(PlaceholderType.Int8Vector,
                new Int8Vec(new byte[]{1}).getPlaceholderType());
    }
}
