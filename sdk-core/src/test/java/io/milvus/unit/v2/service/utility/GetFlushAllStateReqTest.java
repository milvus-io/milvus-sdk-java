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

import io.milvus.v2.service.utility.request.GetFlushAllStateReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class GetFlushAllStateReqTest {
    @Test
    void builderBuildsAllFields() {
        GetFlushAllStateReq request = GetFlushAllStateReq.builder()
                .databaseName("db")
                .flushAllTs(123456789L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals(Long.valueOf(123456789L), request.getFlushAllTs());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        GetFlushAllStateReq request = GetFlushAllStateReq.builder().build();

        assertNull(request.getDatabaseName());
        assertEquals(Long.valueOf(0L), request.getFlushAllTs());
    }

    @Test
    void settersUpdateFields() {
        GetFlushAllStateReq request = GetFlushAllStateReq.builder().build();

        request.setDatabaseName("db");
        request.setFlushAllTs(99L);

        assertEquals("db", request.getDatabaseName());
        assertEquals(Long.valueOf(99L), request.getFlushAllTs());
    }

    @Test
    void toStringContainsFields() {
        GetFlushAllStateReq request = GetFlushAllStateReq.builder()
                .databaseName("db")
                .flushAllTs(1L)
                .build();

        String text = request.toString();
        assertEquals("GetFlushAllStateReq{databaseName='db', flushAllTs=1}", text);
    }
}
