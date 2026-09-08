/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.utility;

import io.milvus.v2.common.CompactionPlan;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.service.utility.response.GetCompactionPlansResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class GetCompactionPlansRespTest {
    @Test
    void builderBuildsAllFields() {
        CompactionPlan plan = CompactionPlan.builder().target(10L).sources(List.of(1L, 2L)).build();
        List<CompactionPlan> plans = List.of(plan);

        GetCompactionPlansResp response = GetCompactionPlansResp.builder()
                .compactionId(7L)
                .state(CompactionState.Completed)
                .plans(plans)
                .build();

        assertEquals(Long.valueOf(7L), response.getCompactionId());
        assertEquals(CompactionState.Completed, response.getState());
        assertSame(plans, response.getPlans());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        GetCompactionPlansResp response = GetCompactionPlansResp.builder().build();

        assertNull(response.getCompactionId());
        assertEquals(CompactionState.UndefiedState, response.getState());
        assertEquals(0, response.getPlans().size());
    }

    @Test
    void setterUpdatesCompactionId() {
        GetCompactionPlansResp response = GetCompactionPlansResp.builder().build();

        response.setCompactionId(42L);

        assertEquals(Long.valueOf(42L), response.getCompactionId());
    }

    @Test
    void toStringContainsFields() {
        GetCompactionPlansResp response = GetCompactionPlansResp.builder()
                .compactionId(1L)
                .state(CompactionState.Completed)
                .build();

        String text = response.toString();
        assertEquals("GetCompactionPlansResp{compactionId=1, state=Completed, plans=[]}", text);
    }
}
