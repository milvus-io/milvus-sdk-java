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

package io.milvus.bulkwriter.common.clientenum;

import io.milvus.exception.ParamException;
import io.milvus.v2.common.DataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class TypeSizeTest {

    @Test
    void testContains() {
        assertTrue(TypeSize.contains(DataType.Bool));
        assertTrue(TypeSize.contains(DataType.Int8));
        assertTrue(TypeSize.contains(DataType.Int16));
        assertTrue(TypeSize.contains(DataType.Int32));
        assertTrue(TypeSize.contains(DataType.Int64));
        assertTrue(TypeSize.contains(DataType.Float));
        assertTrue(TypeSize.contains(DataType.Double));
        assertFalse(TypeSize.contains(DataType.VarChar));
        assertFalse(TypeSize.contains(DataType.FloatVector));
    }

    @Test
    void testGetSize() {
        assertEquals(1, TypeSize.getSize(DataType.Bool));
        assertEquals(1, TypeSize.getSize(DataType.Int8));
        assertEquals(2, TypeSize.getSize(DataType.Int16));
        assertEquals(4, TypeSize.getSize(DataType.Int32));
        assertEquals(8, TypeSize.getSize(DataType.Int64));
        assertEquals(4, TypeSize.getSize(DataType.Float));
        assertEquals(8, TypeSize.getSize(DataType.Double));
    }

    @Test
    void testGetSizeRejectsUnmappedDataType() {
        assertThrows(ParamException.class, () -> TypeSize.getSize(DataType.VarChar));
        assertThrows(ParamException.class, () -> TypeSize.getSize(DataType.Array));
    }
}
