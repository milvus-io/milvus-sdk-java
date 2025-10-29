package io.milvus.v2.service.collection;

import java.util.ArrayList;
import java.util.List;

public class ShardReplica {
    private Long leaderID;
    private String leaderAddress; // IP:port
    private String channelName;
    private List<Long> nodeIDs;

    private ShardReplica(ShardReplicaBuilder builder) {
        this.leaderID = builder.leaderID;
        this.leaderAddress = builder.leaderAddress;
        this.channelName = builder.channelName != null ? builder.channelName : "";
        this.nodeIDs = builder.nodeIDs != null ? builder.nodeIDs : new ArrayList<>();
    }

    public static ShardReplicaBuilder builder() {
        return new ShardReplicaBuilder();
    }

    // Getters
    public Long getLeaderID() {
        return leaderID;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public String getChannelName() {
        return channelName;
    }

    public List<Long> getNodeIDs() {
        return nodeIDs;
    }

    // Setters
    public void setLeaderID(Long leaderID) {
        this.leaderID = leaderID;
    }

    public void setLeaderAddress(String leaderAddress) {
        this.leaderAddress = leaderAddress;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public void setNodeIDs(List<Long> nodeIDs) {
        this.nodeIDs = nodeIDs;
    }

    @Override
    public String toString() {
        return "ShardReplica{" +
                "leaderID=" + leaderID +
                ", leaderAddress='" + leaderAddress + '\'' +
                ", channelName='" + channelName + '\'' +
                ", nodeIDs=" + nodeIDs +
                '}';
    }

    public static class ShardReplicaBuilder {
        private Long leaderID;
        private String leaderAddress;
        private String channelName = "";
        private List<Long> nodeIDs = new ArrayList<>();

        public ShardReplicaBuilder leaderID(Long leaderID) {
            this.leaderID = leaderID;
            return this;
        }

        public ShardReplicaBuilder leaderAddress(String leaderAddress) {
            this.leaderAddress = leaderAddress;
            return this;
        }

        public ShardReplicaBuilder channelName(String channelName) {
            this.channelName = channelName;
            return this;
        }

        public ShardReplicaBuilder nodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
            return this;
        }

        public ShardReplica build() {
            return new ShardReplica(this);
        }
    }
}
