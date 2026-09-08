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

package io.milvus.unit.v2.service.cdc;

import io.milvus.v2.service.cdc.request.DumpMessagesReq;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class DumpMessagesReqTest {
    @Test
    void builderBuildsAllFields() {
        GetReplicateInfoResp.MessageID startMessageID = GetReplicateInfoResp.MessageID.builder()
                .id("id-1")
                .walName("Pulsar")
                .build();

        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("chan-1")
                .startMessageID(startMessageID)
                .startTimetick(100L)
                .endTimetick(200L)
                .includeStartMessage(false)
                .build();

        assertEquals("chan-1", request.getPchannel());
        assertSame(startMessageID, request.getStartMessageID());
        assertEquals(Long.valueOf(100L), request.getStartTimetick());
        assertEquals(Long.valueOf(200L), request.getEndTimetick());
        assertEquals(Boolean.FALSE, request.getIncludeStartMessage());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DumpMessagesReq request = DumpMessagesReq.builder().build();

        assertNull(request.getPchannel());
        assertNull(request.getStartMessageID());
        assertEquals(Long.valueOf(0L), request.getStartTimetick());
        assertEquals(Long.valueOf(0L), request.getEndTimetick());
        assertEquals(Boolean.TRUE, request.getIncludeStartMessage());
    }

    @Test
    void toStringContainsFields() {
        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("chan-1")
                .startTimetick(1L)
                .endTimetick(2L)
                .includeStartMessage(true)
                .build();

        String text = request.toString();
        assertEquals("DumpMessagesReq{pchannel='chan-1', startMessageID=null, startTimetick=1, endTimetick=2, includeStartMessage=true}", text);
    }
}
