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
import io.milvus.param.resourcegroup.TransferNodeParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class TransferNodeParamTest {
    @Test
    void buildAndGet() {
        TransferNodeParam param = TransferNodeParam.newBuilder()
                .withSourceGroupName("rg1")
                .withTargetGroupName("rg2")
                .withNodeNumber(3)
                .build();

        assertEquals("rg1", param.getSourceGroupName());
        assertEquals("rg2", param.getTargetGroupName());
        assertEquals(3, param.getNodeNumber());
    }

    @Test
    void buildFailsWhenGroupNameMissing() {
        assertThrows(ParamException.class, () -> TransferNodeParam.newBuilder()
                .withTargetGroupName("rg2").withNodeNumber(3).build());
        assertThrows(ParamException.class, () -> TransferNodeParam.newBuilder()
                .withSourceGroupName("rg1").withNodeNumber(3).build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                TransferNodeParam.newBuilder().withSourceGroupName(null));
        assertThrows(IllegalArgumentException.class, () ->
                TransferNodeParam.newBuilder().withTargetGroupName(null));
        assertThrows(IllegalArgumentException.class, () ->
                TransferNodeParam.newBuilder().withNodeNumber(null));
    }
}
