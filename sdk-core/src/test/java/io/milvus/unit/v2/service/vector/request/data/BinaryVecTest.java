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
import io.milvus.v2.service.vector.request.data.BinaryVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class BinaryVecTest {

    @Test
    void byteBufferConstructorPreservesBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{(byte) 0x0f, (byte) 0xf0});
        BinaryVec vec = new BinaryVec(buffer);

        assertSame(buffer, vec.getData());
        assertEquals(2, ((ByteBuffer) vec.getData()).limit());
    }

    @Test
    void byteArrayConstructorWrapsIntoBuffer() {
        BinaryVec vec = new BinaryVec(new byte[]{(byte) 0xff, 0x00, 0x55});

        ByteBuffer data = (ByteBuffer) vec.getData();
        assertEquals(3, data.limit());
        assertEquals((byte) 0xff, data.get(0));
        assertEquals(0x00, data.get(1));
        assertEquals(0x55, data.get(2));
    }

    @Test
    void placeholderTypeIsBinaryVector() {
        assertEquals(PlaceholderType.BinaryVector,
                new BinaryVec(new byte[]{1}).getPlaceholderType());
    }
}
