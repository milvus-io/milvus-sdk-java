package io.milvus.v2.service.collection;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ShardReplica {
    private Long leaderID;
    private String leaderAddress; // IP:port
    private String channelName;
    private List<Long> nodeIDs;

    private ShardReplica(Builder builder) {
        this.leaderID = builder.leaderID;
        this.leaderAddress = builder.leaderAddress;
        this.channelName = builder.channelName != null ? builder.channelName : "";
        this.nodeIDs = builder.nodeIDs != null ? builder.nodeIDs : new ArrayList<>();
    }

    public static Builder builder() {
        return new Builder();
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ShardReplica that = (ShardReplica) obj;
        
        return new EqualsBuilder()
                .append(leaderID, that.leaderID)
                .append(leaderAddress, that.leaderAddress)
                .append(channelName, that.channelName)
                .append(nodeIDs, that.nodeIDs)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderID, leaderAddress, channelName, nodeIDs);
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

    public static class Builder {
        private Long leaderID;
        private String leaderAddress;
        private String channelName = "";
        private List<Long> nodeIDs = new ArrayList<>();

        public Builder leaderID(Long leaderID) {
            this.leaderID = leaderID;
            return this;
        }

        public Builder leaderAddress(String leaderAddress) {
            this.leaderAddress = leaderAddress;
            return this;
        }

        public Builder channelName(String channelName) {
            this.channelName = channelName;
            return this;
        }

        public Builder nodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
            return this;
        }

        public ShardReplica build() {
            return new ShardReplica(this);
        }
    }
}
