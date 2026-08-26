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
    private final Boolean includeStartMessage;

    private DumpMessagesReq(DumpMessagesReqBuilder builder) {
        this.pchannel = builder.pchannel;
        this.startMessageID = builder.startMessageID;
        this.startTimetick = builder.startTimetick;
        this.endTimetick = builder.endTimetick;
        this.includeStartMessage = builder.includeStartMessage;
    }

    /**
     * Creates a new {@code DumpMessagesReq} builder.
     *
     * @return the builder
     */
    public static DumpMessagesReqBuilder builder() {
        return new DumpMessagesReqBuilder();
    }

    /**
     * Returns the physical channel to dump messages from.
     *
     * @return the physical channel
     */
    public String getPchannel() {
        return pchannel;
    }

    /**
     * Start position in WAL. Its walName supports: RocksMQ, Pulsar, Kafka, WoodPecker.
     *
     * @return the start message ID
     */
    public GetReplicateInfoResp.MessageID getStartMessageID() {
        return startMessageID;
    }

    /**
     * Returns the start timetick of the message range to dump.
     *
     * @return the start timetick
     */
    public Long getStartTimetick() {
        return startTimetick;
    }

    /**
     * Returns the end timetick of the message range to dump.
     *
     * @return the end timetick
     */
    public Long getEndTimetick() {
        return endTimetick;
    }

    /**
     * Returns whether the message at the start position is included in the dump.
     *
     * @return {@code true} if the start message is included
     */
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

        /**
         * Sets the physical channel to dump messages from.
         *
         * @param pchannel the physical channel
         * @return this builder
         */
        public DumpMessagesReqBuilder pchannel(String pchannel) {
            this.pchannel = pchannel;
            return this;
        }

        /**
         * Sets the start position in WAL.
         *
         * @param startMessageID the start message ID
         * @return this builder
         */
        public DumpMessagesReqBuilder startMessageID(GetReplicateInfoResp.MessageID startMessageID) {
            this.startMessageID = startMessageID;
            return this;
        }

        /**
         * Sets the start timetick of the message range to dump.
         *
         * @param startTimetick the start timetick
         * @return this builder
         */
        public DumpMessagesReqBuilder startTimetick(Long startTimetick) {
            this.startTimetick = startTimetick;
            return this;
        }

        /**
         * Sets the end timetick of the message range to dump.
         *
         * @param endTimetick the end timetick
         * @return this builder
         */
        public DumpMessagesReqBuilder endTimetick(Long endTimetick) {
            this.endTimetick = endTimetick;
            return this;
        }

        /**
         * Sets whether the message at the start position is included in the dump.
         *
         * @param includeStartMessage {@code true} if the start message is included
         * @return this builder
         */
        public DumpMessagesReqBuilder includeStartMessage(Boolean includeStartMessage) {
            this.includeStartMessage = includeStartMessage;
            return this;
        }

        /**
         * Builds the {@link DumpMessagesReq}.
         *
         * @return the request
         */
        public DumpMessagesReq build() {
            return new DumpMessagesReq(this);
        }
    }
}
