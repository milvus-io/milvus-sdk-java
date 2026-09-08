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

import io.milvus.v2.service.cdc.response.DumpMessageInfo;
import io.milvus.v2.service.cdc.response.DumpMessagesResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DumpMessagesRespTest {
    @Test
    void builderBuildsAllFields() {
        List<DumpMessageInfo> messages = new ArrayList<>();
        messages.add(DumpMessageInfo.builder()
                .messageID(GetReplicateInfoResp.MessageID.builder().id("id-1").walName("RocksMQ").build())
                .build());

        DumpMessagesResp response = DumpMessagesResp.builder()
                .messages(messages)
                .build();

        assertSame(messages, response.getMessages());
    }

    @Test
    void unsetMessagesDefaultsToEmptyIterable() {
        DumpMessagesResp response = DumpMessagesResp.builder().build();

        assertFalse(response.getMessages().iterator().hasNext());
    }

    @Test
    void iteratorIteratesOverMessages() {
        DumpMessageInfo m1 = DumpMessageInfo.builder().build();
        DumpMessageInfo m2 = DumpMessageInfo.builder().build();

        DumpMessagesResp response = DumpMessagesResp.builder()
                .messages(List.of(m1, m2))
                .build();

        int count = 0;
        for (DumpMessageInfo message : response) {
            count++;
        }
        assertEquals(2, count);
        assertTrue(response.iterator().hasNext());
    }

    @Test
    void toStringDoesNotDumpStream() {
        DumpMessagesResp response = DumpMessagesResp.builder()
                .messages(List.of(DumpMessageInfo.builder().build()))
                .build();

        assertEquals("DumpMessagesResp{messages=<stream>}", response.toString());
    }
}
