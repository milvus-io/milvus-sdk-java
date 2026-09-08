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

package io.milvus.unit.v1.param;

import io.milvus.exception.ParamException;
import io.milvus.param.control.GetQuerySegmentInfoParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class GetQuerySegmentInfoParamTest {
    @Test
    void buildAndGet() {
        GetQuerySegmentInfoParam param = GetQuerySegmentInfoParam.newBuilder()
                .withCollectionName("coll1")
                .build();

        assertEquals("coll1", param.getCollectionName());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> GetQuerySegmentInfoParam.newBuilder().build());
    }

    @Test
    void withCollectionNameRejectsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                GetQuerySegmentInfoParam.newBuilder().withCollectionName(null));
    }
}
