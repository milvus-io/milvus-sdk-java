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
     */
    public static GetReplicateInfoRespBuilder builder() {
        return new GetReplicateInfoRespBuilder();
    }

    /**
     * Returns the current replication checkpoint.
     */
    public ReplicateCheckpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Sets the current replication checkpoint.
     */
    public void setCheckpoint(ReplicateCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * Returns the salvage checkpoint captured during force promote. It represents the last synced
     * position from the old primary, and can be used to dump unsynchronized messages from it.
     */
    public ReplicateCheckpoint getSalvageCheckpoint() {
        return salvageCheckpoint;
    }

    /**
     * Sets the salvage checkpoint captured during force promote.
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

    /**
     * Builder for {@link GetReplicateInfoResp}.
     */
    public static class GetReplicateInfoRespBuilder {
        private ReplicateCheckpoint checkpoint;
        private ReplicateCheckpoint salvageCheckpoint;

        /**
         * Sets the current replication checkpoint.
         */
        public GetReplicateInfoRespBuilder checkpoint(ReplicateCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        /**
         * Sets the salvage checkpoint captured during force promote.
         */
        public GetReplicateInfoRespBuilder salvageCheckpoint(ReplicateCheckpoint salvageCheckpoint) {
            this.salvageCheckpoint = salvageCheckpoint;
            return this;
        }

        /**
         * Builds the {@link GetReplicateInfoResp}.
         */
        public GetReplicateInfoResp build() {
            return new GetReplicateInfoResp(this);
        }
    }

    /**
     * A replication checkpoint identifying the position of replicated data. It records the cluster
     * and the physical channel of the source cluster, together with the last confirmed message.
     */
    public static class ReplicateCheckpoint {
        private String clusterId;
        private String pchannel;
        private MessageID messageID;
        private Long timeTick;

        /**
         * Converts a gRPC {@code ReplicateCheckpoint} to this class.
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
         */
        public static ReplicateCheckpointBuilder builder() {
            return new ReplicateCheckpointBuilder();
        }

        /**
         * Returns the cluster ID of the source cluster.
         */
        public String getClusterId() {
            return clusterId;
        }

        /**
         * Sets the cluster ID of the source cluster.
         */
        public void setClusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        /**
         * Returns the pchannel of the source cluster.
         */
        public String getPchannel() {
            return pchannel;
        }

        /**
         * Sets the pchannel of the source cluster.
         */
        public void setPchannel(String pchannel) {
            this.pchannel = pchannel;
        }

        /**
         * Returns the last confirmed message ID of the last replicated message.
         */
        public MessageID getMessageID() {
            return messageID;
        }

        /**
         * Sets the last confirmed message ID of the last replicated message.
         */
        public void setMessageID(MessageID messageID) {
            this.messageID = messageID;
        }

        /**
         * Returns the time tick of the last replicated message.
         */
        public Long getTimeTick() {
            return timeTick;
        }

        /**
         * Sets the time tick of the last replicated message.
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

        /**
         * Builder for {@link ReplicateCheckpoint}.
         */
        public static class ReplicateCheckpointBuilder {
            private String clusterId;
            private String pchannel;
            private MessageID messageID;
            private Long timeTick;

            /**
             * Sets the cluster ID of the source cluster.
             */
            public ReplicateCheckpointBuilder clusterId(String clusterId) {
                this.clusterId = clusterId;
                return this;
            }

            /**
             * Sets the pchannel of the source cluster.
             */
            public ReplicateCheckpointBuilder pchannel(String pchannel) {
                this.pchannel = pchannel;
                return this;
            }

            /**
             * Sets the last confirmed message ID of the last replicated message.
             */
            public ReplicateCheckpointBuilder messageID(MessageID messageID) {
                this.messageID = messageID;
                return this;
            }

            /**
             * Sets the time tick of the last replicated message.
             */
            public ReplicateCheckpointBuilder timeTick(Long timeTick) {
                this.timeTick = timeTick;
                return this;
            }

            /**
             * Builds the {@link ReplicateCheckpoint}.
             */
            public ReplicateCheckpoint build() {
                return new ReplicateCheckpoint(this);
            }
        }
    }

    /**
     * A message ID identifying a position in the WAL, consisting of an opaque ID and the WAL
     * implementation name.
     */
    public static class MessageID {
        private String id;
        /**
         * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
         */
        private String walName;

        /**
         * Converts a gRPC {@code MessageID} to this class.
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
         */
        public static MessageIDBuilder builder() {
            return new MessageIDBuilder();
        }

        /**
         * Returns the opaque message ID.
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the opaque message ID.
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
         */
        public String getWalName() {
            return walName;
        }

        /**
         * Sets the WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
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

        /**
         * Builder for {@link MessageID}.
         */
        public static class MessageIDBuilder {
            private String id;
            private String walName;

            /**
             * Sets the opaque message ID.
             */
            public MessageIDBuilder id(String id) {
                this.id = id;
                return this;
            }

            /**
             * WAL implementation name. Supported values: RocksMQ, Pulsar, Kafka, WoodPecker.
             */
            public MessageIDBuilder walName(String walName) {
                this.walName = walName;
                return this;
            }

            /**
             * Builds the {@link MessageID}.
             */
            public MessageID build() {
                return new MessageID(this);
            }
        }
    }
}
