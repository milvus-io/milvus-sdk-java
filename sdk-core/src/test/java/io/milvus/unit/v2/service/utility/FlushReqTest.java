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

import io.milvus.v2.service.utility.request.FlushReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class FlushReqTest {
    @Test
    void builderBuildsAllFields() {
        List<String> collections = new ArrayList<>();
        collections.add("coll1");
        collections.add("coll2");

        FlushReq request = FlushReq.builder()
                .databaseName("db")
                .collectionNames(collections)
                .waitFlushedTimeoutMs(5000L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertSame(collections, request.getCollectionNames());
        assertEquals(Long.valueOf(5000L), request.getWaitFlushedTimeoutMs());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        FlushReq request = FlushReq.builder().build();

        assertNull(request.getDatabaseName());
        assertEquals(0, request.getCollectionNames().size());
        assertEquals(Long.valueOf(0L), request.getWaitFlushedTimeoutMs());
    }

    @Test
    void settersUpdateFields() {
        FlushReq request = FlushReq.builder().build();

        request.setDatabaseName("db");
        request.setCollectionNames(Arrays.asList("coll"));
        request.setWaitFlushedTimeoutMs(1L);

        assertEquals("db", request.getDatabaseName());
        assertEquals(Arrays.asList("coll"), request.getCollectionNames());
        assertEquals(Long.valueOf(1L), request.getWaitFlushedTimeoutMs());
    }

    @Test
    void waitFlushedTimeoutMsSupportsZeroWaitUntilDone() {
        FlushReq request = FlushReq.builder()
                .waitFlushedTimeoutMs(0L)
                .build();

        assertEquals(Long.valueOf(0L), request.getWaitFlushedTimeoutMs());
    }

    @Test
    void toStringContainsFields() {
        FlushReq request = FlushReq.builder()
                .databaseName("db")
                .collectionNames(Arrays.asList("coll"))
                .waitFlushedTimeoutMs(1L)
                .build();

        String text = request.toString();
        assertEquals("FlushReq{databaseName='db', collectionNames=[coll], waitFlushedTimeoutMs=1}", text);
    }
}
