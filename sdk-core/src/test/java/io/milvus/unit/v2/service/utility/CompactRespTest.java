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

import io.milvus.v2.service.utility.response.CompactResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class CompactRespTest {
    @Test
    void builderBuildsAllFields() {
        CompactResp response = CompactResp.builder()
                .compactionID(123L)
                .build();

        assertEquals(Long.valueOf(123L), response.getCompactionID());
    }

    @Test
    void unsetFieldDefaultsToZero() {
        CompactResp response = CompactResp.builder().build();

        assertEquals(Long.valueOf(0L), response.getCompactionID());
    }

    @Test
    void setterUpdatesField() {
        CompactResp response = CompactResp.builder().build();

        response.setCompactionID(42L);

        assertEquals(Long.valueOf(42L), response.getCompactionID());
    }

    @Test
    void toStringContainsFields() {
        CompactResp response = CompactResp.builder()
                .compactionID(1L)
                .build();

        String text = response.toString();
        assertEquals("CompactResp{compactionID=1}", text);
    }
}
