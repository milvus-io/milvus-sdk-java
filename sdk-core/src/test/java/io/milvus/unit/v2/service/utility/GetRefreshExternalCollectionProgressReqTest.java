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

import io.milvus.v2.service.utility.request.GetRefreshExternalCollectionProgressReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class GetRefreshExternalCollectionProgressReqTest {
    @Test
    void builderBuildsAllFields() {
        GetRefreshExternalCollectionProgressReq request = GetRefreshExternalCollectionProgressReq.builder()
                .jobId(42L)
                .build();

        assertEquals(42L, request.getJobId());
    }

    @Test
    void unsetFieldDefaultsToZero() {
        GetRefreshExternalCollectionProgressReq request = GetRefreshExternalCollectionProgressReq.builder().build();

        assertEquals(0L, request.getJobId());
    }

    @Test
    void supportsNegativeJobId() {
        GetRefreshExternalCollectionProgressReq request = GetRefreshExternalCollectionProgressReq.builder()
                .jobId(-1L)
                .build();

        assertEquals(-1L, request.getJobId());
    }

    @Test
    void toStringContainsFields() {
        GetRefreshExternalCollectionProgressReq request = GetRefreshExternalCollectionProgressReq.builder()
                .jobId(7L)
                .build();

        String text = request.toString();
        assertEquals("GetRefreshExternalCollectionProgressReq{jobId=7}", text);
    }
}
