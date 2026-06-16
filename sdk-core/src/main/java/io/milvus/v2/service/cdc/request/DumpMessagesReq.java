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

package io.milvus.v2.service.cdc.request;

import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;

public class DumpMessagesReq {
    private final String pchannel;
    private final GetReplicateInfoResp.MessageID startMessageID;
    private final Long startTimetick;
    private final Long endTimetick;
    private final Boolean includeStartMessage;

    private DumpMessagesReq(DumpMessagesReqBuilder builder) {
        this.pchannel = builder.pchannel;
        this.startMessageID = builder.startMessageID;
        this.startTimetick = builder.startTimetick;
        this.endTimetick = builder.endTimetick;
        this.includeStartMessage = builder.includeStartMessage;
    }

    public static DumpMessagesReqBuilder builder() {
        return new DumpMessagesReqBuilder();
    }

    public String getPchannel() {
        return pchannel;
    }

    /**
     * Start position in WAL. Its walName supports: RocksMQ, Pulsar, Kafka, WoodPecker.
     */
    public GetReplicateInfoResp.MessageID getStartMessageID() {
        return startMessageID;
    }

    public Long getStartTimetick() {
        return startTimetick;
    }

    public Long getEndTimetick() {
        return endTimetick;
    }

    public Boolean getIncludeStartMessage() {
        return includeStartMessage;
    }

    @Override
    public String toString() {
        return "DumpMessagesReq{" +
                "pchannel='" + pchannel + '\'' +
                ", startMessageID=" + startMessageID +
                ", startTimetick=" + startTimetick +
                ", endTimetick=" + endTimetick +
                ", includeStartMessage=" + includeStartMessage +
                '}';
    }

    public static class DumpMessagesReqBuilder {
        private String pchannel;
        private GetReplicateInfoResp.MessageID startMessageID;
        private Long startTimetick = 0L;
        private Long endTimetick = 0L;
        private Boolean includeStartMessage = Boolean.TRUE;

        public DumpMessagesReqBuilder pchannel(String pchannel) {
            this.pchannel = pchannel;
            return this;
        }

        public DumpMessagesReqBuilder startMessageID(GetReplicateInfoResp.MessageID startMessageID) {
            this.startMessageID = startMessageID;
            return this;
        }

        public DumpMessagesReqBuilder startTimetick(Long startTimetick) {
            this.startTimetick = startTimetick;
            return this;
        }

        public DumpMessagesReqBuilder endTimetick(Long endTimetick) {
            this.endTimetick = endTimetick;
            return this;
        }

        public DumpMessagesReqBuilder includeStartMessage(Boolean includeStartMessage) {
            this.includeStartMessage = includeStartMessage;
            return this;
        }

        public DumpMessagesReq build() {
            return new DumpMessagesReq(this);
        }
    }
}
