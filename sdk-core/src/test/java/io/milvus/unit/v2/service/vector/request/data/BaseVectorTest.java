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
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("unit")
class BaseVectorTest {

    @Test
    void allVectorTypesExposePlaceholderTypeAndData() {
        BaseVector[] vectors = {
                new FloatVec(Arrays.asList(1.0f, 2.0f)),
                new FloatVec(new float[]{1.0f, 2.0f}),
                new Float16Vec(ByteBuffer.wrap(new byte[]{0, 1, 2, 3})),
                new BFloat16Vec(ByteBuffer.wrap(new byte[]{0, 1, 2, 3})),
                new Int8Vec(new byte[]{1, 2}),
                new BinaryVec(new byte[]{1, 2}),
                new SparseFloatVec(new TreeMap<>(Collections.singletonMap(0L, 1.0f))),
                new EmbeddedText("text")
        };

        for (BaseVector vector : vectors) {
            PlaceholderType type = vector.getPlaceholderType();
            assertNotNull(type);
            assertFalse(type == PlaceholderType.None);
            assertNotNull(vector.getData());
            assertEquals(type, vector.getPlaceholderType());
        }
    }

    @Test
    void placeholderTypesMatchVectorKinds() {
        assertEquals(PlaceholderType.FloatVector,
                new FloatVec(Arrays.asList(1.0f)).getPlaceholderType());
        assertEquals(PlaceholderType.Float16Vector,
                new Float16Vec(ByteBuffer.wrap(new byte[]{0, 0})).getPlaceholderType());
        assertEquals(PlaceholderType.BFloat16Vector,
                new BFloat16Vec(ByteBuffer.wrap(new byte[]{0, 0})).getPlaceholderType());
        assertEquals(PlaceholderType.Int8Vector,
                new Int8Vec(new byte[]{1}).getPlaceholderType());
        assertEquals(PlaceholderType.BinaryVector,
                new BinaryVec(new byte[]{1}).getPlaceholderType());
        assertEquals(PlaceholderType.SparseFloatVector,
                new SparseFloatVec(new TreeMap<>()).getPlaceholderType());
        assertEquals(PlaceholderType.VarChar, new EmbeddedText("x").getPlaceholderType());
    }
}
