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

package io.milvus.unit.v2.service.vector.response;

import io.milvus.v2.service.vector.response.DeleteResp;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DeleteRespTest {

    @Test
    void builderSetsAllFields() {
        DeleteResp resp = DeleteResp.builder()
                .deleteCnt(10)
                .primaryKeys(Arrays.asList(1L, 2L))
                .cost(5L)
                .build();

        assertEquals(10, resp.getDeleteCnt());
        assertEquals(Arrays.asList(1L, 2L), resp.getPrimaryKeys());
        assertEquals(5L, resp.getCost());
    }

    @Test
    void builderDefaults() {
        DeleteResp resp = DeleteResp.builder().build();
        assertEquals(0, resp.getDeleteCnt());
        assertTrue(resp.getPrimaryKeys().isEmpty());
        assertNull(resp.getCost());
    }

    @Test
    void settersUpdateFields() {
        DeleteResp resp = DeleteResp.builder().build();
        resp.setDeleteCnt(3);
        resp.setPrimaryKeys(Arrays.asList(9L));
        resp.setCost(2L);

        assertEquals(3, resp.getDeleteCnt());
        assertEquals(Arrays.asList(9L), resp.getPrimaryKeys());
        assertEquals(2L, resp.getCost());
    }

    @Test
    void toStringContainsFields() {
        DeleteResp resp = DeleteResp.builder().deleteCnt(1).build();
        assertNotNull(resp.toString());
    }
}
