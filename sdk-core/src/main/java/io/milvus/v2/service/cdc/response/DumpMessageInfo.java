/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
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

package io.milvus.v2.service.cdc.response;

import java.util.Collections;
import java.util.Map;

public class DumpMessageInfo {
    private final GetReplicateInfoResp.MessageID messageID;
    private final byte[] payload;
    private final Map<String, String> properties;

    private DumpMessageInfo(DumpMessageInfoBuilder builder) {
        this.messageID = builder.messageID;
        this.payload = builder.payload != null ? builder.payload : new byte[0];
        this.properties = builder.properties != null ? builder.properties : Collections.emptyMap();
    }

    public static DumpMessageInfoBuilder builder() {
        return new DumpMessageInfoBuilder();
    }

    public GetReplicateInfoResp.MessageID getMessageID() {
        return messageID;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "DumpMessageInfo{" +
                "messageID=" + messageID +
                ", payloadLength=" + payload.length +
                ", properties=" + properties +
                '}';
    }

    public static class DumpMessageInfoBuilder {
        private GetReplicateInfoResp.MessageID messageID;
        private byte[] payload;
        private Map<String, String> properties;

        public DumpMessageInfoBuilder messageID(GetReplicateInfoResp.MessageID messageID) {
            this.messageID = messageID;
            return this;
        }

        public DumpMessageInfoBuilder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public DumpMessageInfoBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public DumpMessageInfo build() {
            return new DumpMessageInfo(this);
        }
    }
}
