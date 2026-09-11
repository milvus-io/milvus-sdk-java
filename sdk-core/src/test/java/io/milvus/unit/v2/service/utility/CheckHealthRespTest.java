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

import io.milvus.v2.service.utility.response.CheckHealthResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class CheckHealthRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> reasons = Arrays.asList("some reason");
        List<String> quotaStates = Arrays.asList("throttled");

        CheckHealthResp response = CheckHealthResp.builder()
                .isHealthy(true)
                .reasons(reasons)
                .quotaStates(quotaStates)
                .build();

        assertEquals(Boolean.TRUE, response.getIsHealthy());
        assertSame(reasons, response.getReasons());
        assertSame(quotaStates, response.getQuotaStates());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        CheckHealthResp response = CheckHealthResp.builder().build();

        assertFalse(response.getIsHealthy());
        assertEquals(0, response.getReasons().size());
        assertEquals(0, response.getQuotaStates().size());
    }

    @Test
    void settersUpdateFields() {
        CheckHealthResp response = CheckHealthResp.builder().build();

        response.setIsHealthy(true);
        response.setReasons(Arrays.asList("r"));
        response.setQuotaStates(Arrays.asList("q"));

        assertEquals(Boolean.TRUE, response.getIsHealthy());
        assertEquals(Arrays.asList("r"), response.getReasons());
        assertEquals(Arrays.asList("q"), response.getQuotaStates());
    }

    @Test
    void toStringContainsFields() {
        CheckHealthResp response = CheckHealthResp.builder()
                .isHealthy(true)
                .reasons(Arrays.asList("r"))
                .quotaStates(Arrays.asList("q"))
                .build();

        String text = response.toString();
        assertEquals("CheckHealthResp{isHealthy=true, reasons=[r], quotaStates=[q]}", text);
    }
}
