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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.DataUtils;

import io.milvus.grpc.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tag("unit")
class DataUtilsVectorArrayTest {

    @Test
    void testGenVectorArray() {
        List<List<List<Float>>> rows = Collections.singletonList(Arrays.asList(
                Arrays.asList(1.0f, 2.0f),
                Arrays.asList(3.0f, 4.0f)));

        VectorArray array = DataUtils.genVectorArray(io.milvus.grpc.DataType.FloatVector, rows, 2);

        Assertions.assertEquals(io.milvus.grpc.DataType.FloatVector, array.getElementType());
        Assertions.assertEquals(2, array.getDim());
        Assertions.assertEquals(1, array.getDataCount());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f),
                array.getData(0).getFloatVector().getDataList());
    }
}
