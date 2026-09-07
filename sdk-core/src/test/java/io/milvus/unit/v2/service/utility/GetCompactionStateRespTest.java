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

import io.milvus.v2.common.CompactionState;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class GetCompactionStateRespTest {
    @Test
    void builderBuildsAllFields() {
        GetCompactionStateResp response = GetCompactionStateResp.builder()
                .state(CompactionState.Completed)
                .executingPlanNo(1L)
                .timeoutPlanNo(2L)
                .completedPlanNo(3L)
                .build();

        assertEquals(CompactionState.Completed, response.getState());
        assertEquals(Long.valueOf(1L), response.getExecutingPlanNo());
        assertEquals(Long.valueOf(2L), response.getTimeoutPlanNo());
        assertEquals(Long.valueOf(3L), response.getCompletedPlanNo());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        GetCompactionStateResp response = GetCompactionStateResp.builder().build();

        assertEquals(CompactionState.UndefiedState, response.getState());
        assertEquals(Long.valueOf(0L), response.getExecutingPlanNo());
        assertEquals(Long.valueOf(0L), response.getTimeoutPlanNo());
        assertEquals(Long.valueOf(0L), response.getCompletedPlanNo());
    }

    @Test
    void settersUpdateFields() {
        GetCompactionStateResp response = GetCompactionStateResp.builder().build();

        response.setState(CompactionState.Executing);
        response.setExecutingPlanNo(4L);
        response.setTimeoutPlanNo(5L);
        response.setCompletedPlanNo(6L);

        assertEquals(CompactionState.Executing, response.getState());
        assertEquals(Long.valueOf(4L), response.getExecutingPlanNo());
        assertEquals(Long.valueOf(5L), response.getTimeoutPlanNo());
        assertEquals(Long.valueOf(6L), response.getCompletedPlanNo());
    }

    @Test
    void coversAllCompactionStates() {
        assertEquals(CompactionState.UndefiedState.getCode(), 0);
        assertEquals(CompactionState.Executing.getCode(), 1);
        assertEquals(CompactionState.Completed.getCode(), 2);
    }

    @Test
    void toStringContainsFields() {
        GetCompactionStateResp response = GetCompactionStateResp.builder()
                .state(CompactionState.Completed)
                .executingPlanNo(1L)
                .timeoutPlanNo(2L)
                .completedPlanNo(3L)
                .build();

        String text = response.toString();
        assertEquals("GetCompactionStateResp{state=Completed, executingPlanNo=1, timeoutPlanNo=2, completedPlanNo=3}", text);
    }
}
