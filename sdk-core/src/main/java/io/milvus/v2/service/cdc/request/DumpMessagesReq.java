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

/**
 * Request parameters for the {@code dumpMessages} CDC API.
 */
public class DumpMessagesReq {
    private final String pchannel;
    private final GetReplicateInfoResp.MessageID startMessageID;
    private final Long startTimetick;
    private final Long endTimetick;

    private DumpMessagesReq(DumpMessagesReqBuilder builder) {
        this.pchannel = builder.pchannel;
        this.startMessageID = builder.startMessageID;
        this.startTimetick = builder.startTimetick;
        this.endTimetick = builder.endTimetick;
    }

    /**
     * Creates a new {@code DumpMessagesReq} builder.
     */
    public static DumpMessagesReqBuilder builder() {
        return new DumpMessagesReqBuilder();
    }

    /**
     * Returns the physical channel to dump messages from.
     */
    public String getPchannel() {
        return pchannel;
    }

    /**
     * Start position in WAL. Its walName supports: RocksMQ, Pulsar, Kafka, WoodPecker.
     */
    public GetReplicateInfoResp.MessageID getStartMessageID() {
        return startMessageID;
    }

    /**
     * Returns the start timetick filter; only messages with timetick >= startTimetick are dumped.
     */
    public Long getStartTimetick() {
        return startTimetick;
    }

    /**
     * Returns the end timetick filter; only messages with timetick <= endTimetick are dumped. 0 means no limit.
     */
    public Long getEndTimetick() {
        return endTimetick;
    }

    @Override
    public String toString() {
        return "DumpMessagesReq{" +
                "pchannel='" + pchannel + '\'' +
                ", startMessageID=" + startMessageID +
                ", startTimetick=" + startTimetick +
                ", endTimetick=" + endTimetick +
                '}';
    }

    /**
     * Builder for {@link DumpMessagesReq}.
     */
    public static class DumpMessagesReqBuilder {
        private String pchannel;
        private GetReplicateInfoResp.MessageID startMessageID;
        private Long startTimetick = 0L;
        private Long endTimetick = 0L;

        /**
         * Sets the physical channel to dump messages from.
         */
        public DumpMessagesReqBuilder pchannel(String pchannel) {
            this.pchannel = pchannel;
            return this;
        }

        /**
         * Sets the start position in WAL.
         */
        public DumpMessagesReqBuilder startMessageID(GetReplicateInfoResp.MessageID startMessageID) {
            this.startMessageID = startMessageID;
            return this;
        }

        /**
         * Sets the start timetick filter; only messages with timetick >= startTimetick are dumped.
         */
        public DumpMessagesReqBuilder startTimetick(Long startTimetick) {
            this.startTimetick = startTimetick;
            return this;
        }

        /**
         * Sets the end timetick filter; only messages with timetick <= endTimetick are dumped. 0 means no limit.
         */
        public DumpMessagesReqBuilder endTimetick(Long endTimetick) {
            this.endTimetick = endTimetick;
            return this;
        }

        /**
         * Builds the {@link DumpMessagesReq}.
         */
        public DumpMessagesReq build() {
            return new DumpMessagesReq(this);
        }
    }
}
