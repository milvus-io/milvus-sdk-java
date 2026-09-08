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
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class EmbeddingListTest {

    @Test
    void emptyListPlaceholderTypeIsNone() {
        EmbeddingList list = new EmbeddingList();
        assertEquals(PlaceholderType.None, list.getPlaceholderType());
    }

    @Test
    void emptyListGetDataThrows() {
        EmbeddingList list = new EmbeddingList();
        assertThrows(MilvusClientException.class, list::getData);
    }

    @Test
    void addRejectsDifferentVectorTypes() {
        EmbeddingList list = new EmbeddingList();
        list.add(new FloatVec(Arrays.asList(1.0f)));
        assertThrows(MilvusClientException.class, () -> list.add(new BinaryVec(new byte[]{1})));
    }

    @Test
    void floatVectorsFlattenIntoSingleList() {
        EmbeddingList list = new EmbeddingList();
        list.add(new FloatVec(Arrays.asList(1.0f, 2.0f)));
        list.add(new FloatVec(Arrays.asList(3.0f, 4.0f)));

        assertEquals(PlaceholderType.EmbListFloatVector, list.getPlaceholderType());
        List<?> data = (List<?>) list.getData();
        assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f), data);
    }

    @Test
    void binaryVectorsMergeIntoBuffer() {
        EmbeddingList list = new EmbeddingList();
        list.add(new BinaryVec(new byte[]{1, 2}));
        list.add(new BinaryVec(new byte[]{3, 4, 5}));

        assertEquals(PlaceholderType.EmbListBinaryVector, list.getPlaceholderType());
        ByteBuffer data = (ByteBuffer) list.getData();
        assertEquals(5, data.limit());
        assertEquals(1, data.get(0));
        assertEquals(2, data.get(1));
        assertEquals(3, data.get(2));
        assertEquals(5, data.get(4));
    }

    @Test
    void float16VectorsMergeIntoBuffer() {
        EmbeddingList list = new EmbeddingList();
        list.add(new Float16Vec(new byte[]{1, 2}));
        list.add(new Float16Vec(new byte[]{3, 4}));

        assertEquals(PlaceholderType.EmbListFloat16Vector, list.getPlaceholderType());
        ByteBuffer data = (ByteBuffer) list.getData();
        assertEquals(4, data.limit());
        assertEquals(1, data.get(0));
        assertEquals(4, data.get(3));
    }

    @Test
    void bfloat16VectorsMergeIntoBuffer() {
        EmbeddingList list = new EmbeddingList();
        list.add(new BFloat16Vec(new byte[]{1, 2}));
        list.add(new BFloat16Vec(new byte[]{3, 4}));

        assertEquals(PlaceholderType.EmbListBFloat16Vector, list.getPlaceholderType());
        ByteBuffer data = (ByteBuffer) list.getData();
        assertEquals(4, data.limit());
    }

    @Test
    void int8VectorsMergeIntoBuffer() {
        EmbeddingList list = new EmbeddingList();
        list.add(new Int8Vec(new byte[]{1}));
        list.add(new Int8Vec(new byte[]{2, 3}));

        assertEquals(PlaceholderType.EmbListInt8Vector, list.getPlaceholderType());
        ByteBuffer data = (ByteBuffer) list.getData();
        assertEquals(3, data.limit());
        assertEquals(1, data.get(0));
        assertEquals(3, data.get(2));
    }

    @Test
    void sparseFloatPlaceholderTypeSupportedButDataUnsupported() {
        EmbeddingList list = new EmbeddingList();
        list.add(new SparseFloatVec(new TreeMap<>(Collections.singletonMap(0L, 1.0f))));

        assertEquals(PlaceholderType.EmbListSparseFloatVector, list.getPlaceholderType());
        assertThrows(MilvusClientException.class, list::getData);
    }
}
