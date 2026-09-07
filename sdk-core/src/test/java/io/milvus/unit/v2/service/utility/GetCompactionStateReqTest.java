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

import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class GetCompactionStateReqTest {
    @Test
    void builderBuildsAllFields() {
        GetCompactionStateReq request = GetCompactionStateReq.builder()
                .compactionID(123L)
                .build();

        assertEquals(Long.valueOf(123L), request.getCompactionID());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        GetCompactionStateReq request = GetCompactionStateReq.builder().build();

        assertNull(request.getCompactionID());
    }

    @Test
    void setterUpdatesField() {
        GetCompactionStateReq request = GetCompactionStateReq.builder().build();

        request.setCompactionID(456L);

        assertEquals(Long.valueOf(456L), request.getCompactionID());
    }

    @Test
    void toStringContainsFields() {
        GetCompactionStateReq request = GetCompactionStateReq.builder()
                .compactionID(1L)
                .build();

        String text = request.toString();
        assertEquals("GetCompactionStateReq{compactionID=1}", text);
    }
}
