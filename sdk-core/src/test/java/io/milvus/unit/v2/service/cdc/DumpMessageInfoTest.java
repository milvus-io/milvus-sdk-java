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
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class DumpMessageInfoTest {
    @Test
    void builderBuildsAllFields() {
        GetReplicateInfoResp.MessageID messageID = GetReplicateInfoResp.MessageID.builder()
                .id("id-1")
                .walName("Pulsar")
                .build();
        byte[] payload = new byte[]{1, 2, 3};
        Map<String, String> properties = Collections.singletonMap("key", "value");

        DumpMessageInfo info = DumpMessageInfo.builder()
                .messageID(messageID)
                .payload(payload)
                .properties(properties)
                .build();

        assertSame(messageID, info.getMessageID());
        assertArrayEquals(payload, info.getPayload());
        assertSame(properties, info.getProperties());
    }

    @Test
    void unsetPayloadDefaultsToEmptyByteArray() {
        DumpMessageInfo info = DumpMessageInfo.builder().build();

        assertNull(info.getMessageID());
        assertEquals(0, info.getPayload().length);
        assertEquals(0, info.getProperties().size());
    }

    @Test
    void nullPayloadAndPropertiesAreNormalized() {
        DumpMessageInfo info = DumpMessageInfo.builder()
                .payload(null)
                .properties(null)
                .build();

        assertEquals(0, info.getPayload().length);
        assertEquals(0, info.getProperties().size());
    }

    @Test
    void payloadIsCopiedByReference() {
        byte[] payload = new byte[]{5, 6, 7};
        DumpMessageInfo info = DumpMessageInfo.builder()
                .payload(payload)
                .build();

        assertSame(payload, info.getPayload());
    }

    @Test
    void toStringContainsPayloadLength() {
        DumpMessageInfo info = DumpMessageInfo.builder()
                .payload(new byte[]{1, 2})
                .build();

        String text = info.toString();
        assertEquals("DumpMessageInfo{messageID=null, payloadLength=2, properties={}}", text);
    }
}
