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

import io.milvus.v2.service.utility.response.GetFlushAllStateResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("unit")
class GetFlushAllStateRespTest {
    @Test
    void builderBuildsAllFields() {
        GetFlushAllStateResp response = GetFlushAllStateResp.builder()
                .flushed(true)
                .build();

        assertEquals(Boolean.TRUE, response.getFlushed());
    }

    @Test
    void unsetFieldDefaultsToFalse() {
        GetFlushAllStateResp response = GetFlushAllStateResp.builder().build();

        assertFalse(response.getFlushed());
    }

    @Test
    void setterUpdatesField() {
        GetFlushAllStateResp response = GetFlushAllStateResp.builder().build();

        response.setFlushed(true);

        assertEquals(Boolean.TRUE, response.getFlushed());
    }

    @Test
    void toStringContainsFields() {
        GetFlushAllStateResp response = GetFlushAllStateResp.builder()
                .flushed(true)
                .build();

        String text = response.toString();
        assertEquals("GetFlushAllStateResp{flushed=true}", text);
    }
}
