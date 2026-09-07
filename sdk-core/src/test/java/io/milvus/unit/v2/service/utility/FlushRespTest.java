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

import io.milvus.v2.service.utility.response.FlushResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class FlushRespTest {
    @Test
    void builderBuildsAllFields() {
        Map<String, List<Long>> segmentIDs = new HashMap<>();
        segmentIDs.put("coll", List.of(1L, 2L));
        Map<String, Long> flushTs = new HashMap<>();
        flushTs.put("coll", 100L);

        FlushResp response = FlushResp.builder()
                .databaseName("db")
                .collectionSegmentIDs(segmentIDs)
                .collectionFlushTs(flushTs)
                .build();

        assertEquals("db", response.getDatabaseName());
        assertSame(segmentIDs, response.getCollectionSegmentIDs());
        assertSame(flushTs, response.getCollectionFlushTs());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        FlushResp response = FlushResp.builder().build();

        assertEquals("", response.getDatabaseName());
        assertEquals(0, response.getCollectionSegmentIDs().size());
        assertEquals(0, response.getCollectionFlushTs().size());
    }

    @Test
    void settersUpdateFields() {
        FlushResp response = FlushResp.builder().build();

        Map<String, List<Long>> segmentIDs = new HashMap<>();
        segmentIDs.put("coll", List.of(3L));
        Map<String, Long> flushTs = new HashMap<>();
        flushTs.put("coll", 200L);

        response.setDatabaseName("db");
        response.setCollectionSegmentIDs(segmentIDs);
        response.setCollectionFlushTs(flushTs);

        assertEquals("db", response.getDatabaseName());
        assertEquals(List.of(3L), response.getCollectionSegmentIDs().get("coll"));
        assertEquals(Long.valueOf(200L), response.getCollectionFlushTs().get("coll"));
    }

    @Test
    void toStringContainsFields() {
        FlushResp response = FlushResp.builder()
                .databaseName("db")
                .build();

        String text = response.toString();
        assertEquals("FlushResp{databaseName='db', collectionSegmentIDs={}, collectionFlushTs={}}", text);
    }
}
