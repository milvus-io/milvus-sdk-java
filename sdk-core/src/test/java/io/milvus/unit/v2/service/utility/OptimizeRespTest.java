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

import io.milvus.v2.service.utility.response.OptimizeResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class OptimizeRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> progress = Arrays.asList("merging segments");

        OptimizeResp response = OptimizeResp.builder()
                .status("done")
                .collectionName("coll")
                .compactionId(1L)
                .targetSize("512MB")
                .progress(progress)
                .build();

        assertEquals("done", response.getStatus());
        assertEquals("coll", response.getCollectionName());
        assertEquals(Long.valueOf(1L), response.getCompactionId());
        assertEquals("512MB", response.getTargetSize());
        assertSame(progress, response.getProgress());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        OptimizeResp response = OptimizeResp.builder().build();

        assertNull(response.getStatus());
        assertNull(response.getCollectionName());
        assertNull(response.getCompactionId());
        assertNull(response.getTargetSize());
        assertNull(response.getProgress());
    }

    @Test
    void toStringContainsFields() {
        OptimizeResp response = OptimizeResp.builder()
                .status("done")
                .collectionName("coll")
                .compactionId(1L)
                .targetSize("512MB")
                .progress(Arrays.asList("p"))
                .build();

        String text = response.toString();
        assertEquals("OptimizeResp{status='done', collectionName='coll', compactionId=1, targetSize='512MB', progress=[p]}", text);
    }
}
