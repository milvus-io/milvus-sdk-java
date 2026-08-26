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

/**
 * Response returned by the {@code getReplicateInfo} CDC API.
 */
public class GetReplicateInfoResp {
    private ReplicateCheckpoint checkpoint;
    private ReplicateCheckpoint salvageCheckpoint;

    private GetReplicateInfoResp(GetReplicateInfoRespBuilder builder) {
        this.checkpoint = builder.checkpoint;
        this.salvageCheckpoint = builder.salvageCheckpoint;
    }

    /**
     * Creates a new {@code GetReplicateInfoResp} builder.
     *
     * @return the builder
     */
    public static GetReplicateInfoRespBuilder builder() {
        return new GetReplicateInfoRespBuilder();
    }

    /**
     * Returns the current replication checkpoint.
     *
     * @return the checkpoint
     */
    public ReplicateCheckpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Sets the current replication checkpoint.
     *
     * @param checkpoint the checkpoint
     */
    public void setCheckpoint(ReplicateCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * Returns the salvage checkpoint used for replication recovery.
     *
     * @return the salvage checkpoint
     */
    public ReplicateCheckpoint getSalvageCheckpoint() {
        return salvageCheckpoint;
    }

    /**
     * Sets the salvage checkpoint used for replication recovery.
     *
     * @param salvageCheckpoint the salvage checkpoint
     */
    public void setSalvageCheckpoint(ReplicateCheckpoint salvageCheckpoint) {
        this.salvageCheckpoint = salvageCheckpoint;
    }

    @Override
    public String toString() {
        return "GetReplicateInfoResp{" +
                "checkpoint=" + checkpoint +
                ", salvageCheckpoint=" + salvageCheckpoint +
                '}';
    }

    public static class GetReplicateInfoRespBuilder {
        private ReplicateCheckpoint checkpoint;
        private ReplicateCheckpoint salvageCheckpoint;

        /**
         * Sets the current replication checkpoint.
         *
         * @param checkpoint the checkpoint
         * @return this builder
         */
        public GetReplicateInfoRespBuilder checkpoint(ReplicateCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        /**
         * Sets the salvage checkpoint used for replication recovery.
         *
         * @param salvageCheckpoint the salvage checkpoint
         * @return this builder
         */
        public GetReplicateInfoRespBuilder salvageCheckpoint(ReplicateCheckpoint salvageCheckpoint) {
            this.salvageCheckpoint = salvageCheckpoint;
            return this;
        }

        /**
         * Builds the {@link GetReplicateInfoResp}.
         *
         * @return the response
         */
        public GetReplicateInfoResp build() {
            return new GetReplicateInfoResp(this);
        }
    }

    /**
     * A replication checkpoint identifying the position of replicated data.
     */
    public static class ReplicateCheckpoint {
        private String clusterId;
        private String pchannel;
        private MessageID messageID;
        private Long timeTick;

        /**
         * Converts a gRPC {@code ReplicateCheckpoint} to this class.
         *
         * @param checkpoint the gRPC checkpoint
         * @return the converted checkpoint
         */
        public static ReplicateCheckpoint fromGRPC(io.milvus.grpc.ReplicateCheckpoint checkpoint) {
            return ReplicateCheckpoint.builder()
                    .clusterId(checkpoint.getClusterId())
                    .pchannel(checkpoint.getPchannel())
                    .messageID(checkpoint.hasMessageId() ? MessageID.fromGRPC(checkpoint.getMessageId()) : null)
                    .timeTick(checkpoint.getTimeTick())
                    .build();
        }

        private ReplicateCheckpoint(ReplicateCheckpointBuilder builder) {
            this.clusterId = builder.clusterId;
            this.pchannel = builder.pchannel;
            this.messageID = builder.messageID;
            this.timeTick = builder.timeTick;
        }

        /**
         * Creates a new {@code ReplicateCheckpoint} builder.
         *
         * @return the builder
         */
        public static ReplicateCheckpointBuilder builder() {
            return new ReplicateCheckpointBuilder();
        }

        /**
         * Returns the cluster ID.
         *
         * @return the cluster ID
         */
        public String getClusterId() {
            return clusterId;
        }

        /**
         * Sets the cluster ID.
         *
         * @param clusterId the cluster ID
         */
        public void setClusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        /**
         * Returns the physical channel.
         *
         * @return the physical channel
         */
        public String getPchannel() {
            return pchannel;
        }

        /**
         * Sets the physical channel.
         *
         * @param pchannel the physical channel
         */
        public void setPchannel(String pchannel) {
            this.pchannel = pchannel;
        }

        /**
         * Returns the message ID of the checkpoint.
         *
         * @return the message ID
         */
        public MessageID getMessageID() {
            return messageID;
        }

        /**
         * Sets the message ID of the checkpoint.
         *
         * @param messageID the message ID
         */
        public void setMessageID(MessageID messageID) {
            this.messageID = messageID;
        }

        /**
         * Returns the timetick of the checkpoint.
         *
         * @return the timetick
         */
        public Long getTimeTick() {
            return timeTick;
        }

        /**
         * Sets the timetick of the checkpoint.
         *
         * @param timeTick the timetick
         */
        public void setTimeTick(Long timeTick) {
            this.timeTick = timeTick;
        }

        @Override
        public String toString() {
            return "ReplicateCheckpoint{" +
                    "clusterId='" + clusterId + '\'' +
                    ", pchannel='" + pchannel + '\'' +
                    ", messageID=" + messageID +
                    ", timeTick=" + timeTick +
                    '}';
        }

        public static class ReplicateCheckpointBuilder {
            private String clusterId;
            private String pchannel;
            private MessageID messageID;
            private Long timeTick;

            /**
             * Sets the cluster ID.
             *
             * @param clusterId the cluster ID
             * @return this builder
             */
            public ReplicateCheckpointBuilder clusterId(String clusterId) {
                this.clusterId = clusterId;
                return this;
            }

            /**
             * Sets the physical channel.
             *
             * @param pchannel the physical channel
             * @return this builder
             */
            public ReplicateCheckpointBuilder pchannel(String pchannel) {
                this.pchannel = pchannel;
                return this;
            }

            /**
             * Sets the message ID of the checkpoint.
             *
             * @param messageID the message ID
             * @return this builder
             */
            public ReplicateCheckpointBuilder messageID(MessageID messageID) {
                this.messageID = messageID;
                return this;
            }

            /**
             * Sets the timetick of the checkpoint.
             *
             * @param timeTick the timetick
             * @return this builder
             */
            public ReplicateCheckpointBuilder timeTick(Long timeTick) {
                this.timeTick = timeTick;
                return this;
            }

            /**
             * Builds the {@link ReplicateCheckpoint}.
             *
             * @return the checkpoint
             */
            public ReplicateCheckpoint build() {
                return new ReplicateCheckpoint(this);
            }
        }
    }

    /**
     * A message ID in the WAL.
     */
    public static class MessageID {
        private String id;
        /**
         * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
         */
        private String walName;

        /**
         * Converts a gRPC {@code MessageID} to this class.
         *
         * @param messageID the gRPC message ID
         * @return the converted message ID
         */
        public static MessageID fromGRPC(io.milvus.grpc.MessageID messageID) {
            return MessageID.builder()
                    .id(messageID.getId())
                    .walName(messageID.getWALName().name())
                    .build();
        }

        private MessageID(MessageIDBuilder builder) {
            this.id = builder.id;
            this.walName = builder.walName;
        }

        /**
         * Creates a new {@code MessageID} builder.
         *
         * @return the builder
         */
        public static MessageIDBuilder builder() {
            return new MessageIDBuilder();
        }

        /**
         * Returns the message ID value.
         *
         * @return the message ID value
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the message ID value.
         *
         * @param id the message ID value
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
         *
         * @return the WAL implementation name
         */
        public String getWalName() {
            return walName;
        }

        /**
         * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
         *
         * @param walName the WAL implementation name
         */
        public void setWalName(String walName) {
            this.walName = walName;
        }

        @Override
        public String toString() {
            return "MessageID{" +
                    "id='" + id + '\'' +
                    ", walName='" + walName + '\'' +
                    '}';
        }

        public static class MessageIDBuilder {
            private String id;
            private String walName;

            /**
             * Sets the message ID value.
             *
             * @param id the message ID value
             * @return this builder
             */
            public MessageIDBuilder id(String id) {
                this.id = id;
                return this;
            }

            /**
             * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
             *
             * @param walName the WAL implementation name
             * @return this builder
             */
            public MessageIDBuilder walName(String walName) {
                this.walName = walName;
                return this;
            }

            /**
             * Builds the {@link MessageID}.
             *
             * @return the message ID
             */
            public MessageID build() {
                return new MessageID(this);
            }
        }
    }
}
