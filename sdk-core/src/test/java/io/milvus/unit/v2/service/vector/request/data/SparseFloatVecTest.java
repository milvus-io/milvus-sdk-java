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
import io.milvus.v2.service.vector.request.data.SparseFloatVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class SparseFloatVecTest {

    @Test
    void constructorPreservesSortedMap() {
        SortedMap<Long, Float> map = new TreeMap<>();
        map.put(3L, 0.5f);
        map.put(1L, 0.2f);
        SparseFloatVec vec = new SparseFloatVec(map);

        assertSame(map, vec.getData());
        assertEquals(2, ((SortedMap<?, ?>) vec.getData()).size());
        assertEquals(0.2f, ((SortedMap<Long, Float>) vec.getData()).get(1L));
        assertEquals(0.5f, ((SortedMap<Long, Float>) vec.getData()).get(3L));
    }

    @Test
    void constructorKeepsSortedOrder() {
        SortedMap<Long, Float> map = new TreeMap<>();
        map.put(10L, 1.0f);
        map.put(2L, 0.4f);
        map.put(7L, 0.9f);

        SparseFloatVec vec = new SparseFloatVec(map);
        SortedMap<Long, Float> data = (SortedMap<Long, Float>) vec.getData();
        assertEquals(java.util.Arrays.asList(2L, 7L, 10L), new java.util.ArrayList<>(data.keySet()));
    }

    @Test
    void placeholderTypeIsSparseFloatVector() {
        assertEquals(PlaceholderType.SparseFloatVector,
                new SparseFloatVec(new TreeMap<>()).getPlaceholderType());
    }
}
