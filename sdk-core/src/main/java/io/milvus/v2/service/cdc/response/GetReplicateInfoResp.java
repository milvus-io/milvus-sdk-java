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

public class GetReplicateInfoResp {
    private ReplicateCheckpoint checkpoint;
    private ReplicateCheckpoint salvageCheckpoint;

    private GetReplicateInfoResp(GetReplicateInfoRespBuilder builder) {
        this.checkpoint = builder.checkpoint;
        this.salvageCheckpoint = builder.salvageCheckpoint;
    }

    public static GetReplicateInfoRespBuilder builder() {
        return new GetReplicateInfoRespBuilder();
    }

    public ReplicateCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(ReplicateCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    public ReplicateCheckpoint getSalvageCheckpoint() {
        return salvageCheckpoint;
    }

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

        public GetReplicateInfoRespBuilder checkpoint(ReplicateCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        public GetReplicateInfoRespBuilder salvageCheckpoint(ReplicateCheckpoint salvageCheckpoint) {
            this.salvageCheckpoint = salvageCheckpoint;
            return this;
        }

        public GetReplicateInfoResp build() {
            return new GetReplicateInfoResp(this);
        }
    }

    public static class ReplicateCheckpoint {
        private String clusterId;
        private String pchannel;
        private MessageID messageID;
        private Long timeTick;

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

        public static ReplicateCheckpointBuilder builder() {
            return new ReplicateCheckpointBuilder();
        }

        public String getClusterId() {
            return clusterId;
        }

        public void setClusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        public String getPchannel() {
            return pchannel;
        }

        public void setPchannel(String pchannel) {
            this.pchannel = pchannel;
        }

        public MessageID getMessageID() {
            return messageID;
        }

        public void setMessageID(MessageID messageID) {
            this.messageID = messageID;
        }

        public Long getTimeTick() {
            return timeTick;
        }

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

            public ReplicateCheckpointBuilder clusterId(String clusterId) {
                this.clusterId = clusterId;
                return this;
            }

            public ReplicateCheckpointBuilder pchannel(String pchannel) {
                this.pchannel = pchannel;
                return this;
            }

            public ReplicateCheckpointBuilder messageID(MessageID messageID) {
                this.messageID = messageID;
                return this;
            }

            public ReplicateCheckpointBuilder timeTick(Long timeTick) {
                this.timeTick = timeTick;
                return this;
            }

            public ReplicateCheckpoint build() {
                return new ReplicateCheckpoint(this);
            }
        }
    }

    public static class MessageID {
        private String id;
        private String walName;

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

        public static MessageIDBuilder builder() {
            return new MessageIDBuilder();
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getWalName() {
            return walName;
        }

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

            public MessageIDBuilder id(String id) {
                this.id = id;
                return this;
            }

            public MessageIDBuilder walName(String walName) {
                this.walName = walName;
                return this;
            }

            public MessageID build() {
                return new MessageID(this);
            }
        }
    }
}
