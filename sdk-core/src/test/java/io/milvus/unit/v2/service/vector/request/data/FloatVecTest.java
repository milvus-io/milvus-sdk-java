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
import io.milvus.v2.service.vector.request.data.FloatVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class FloatVecTest {

    @Test
    void listConstructorPreservesData() {
        List<Float> data = Arrays.asList(0.1f, 0.2f, 0.3f);
        FloatVec vec = new FloatVec(data);

        assertSame(data, vec.getData());
        assertEquals(3, ((List<Float>) vec.getData()).size());
        assertEquals(0.2f, ((List<Float>) vec.getData()).get(1));
    }

    @Test
    void arrayConstructorConvertsToList() {
        FloatVec vec = new FloatVec(new float[]{1.0f, 2.5f, -3.5f});

        List<Float> data = (List<Float>) vec.getData();
        assertEquals(3, data.size());
        assertEquals(1.0f, data.get(0));
        assertEquals(2.5f, data.get(1));
        assertEquals(-3.5f, data.get(2));
    }

    @Test
    void placeholderTypeIsFloatVector() {
        assertEquals(PlaceholderType.FloatVector, new FloatVec(Arrays.asList(1.0f)).getPlaceholderType());
    }

    @Test
    void emptyArrayYieldsEmptyList() {
        FloatVec vec = new FloatVec(new float[]{});
        assertEquals(0, ((List<Float>) vec.getData()).size());
    }
}
