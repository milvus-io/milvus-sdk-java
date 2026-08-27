package io.milvus.v2.service.collection;

import java.util.ArrayList;
import java.util.List;

/**
 * A shard replica containing the query nodes that serve a shard channel of a collection.
 */
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

    /**
     * Creates a new {@code ShardReplica} builder.
     *
     * @return the builder
     */
    public static ShardReplicaBuilder builder() {
        return new ShardReplicaBuilder();
    }

    // Getters
    /**
     * Returns the ID of the leader query node of the shard replica.
     *
     * @return the leader query node ID
     */
    public Long getLeaderID() {
        return leaderID;
    }

    /**
     * Returns the address of the leader query node, in {@code IP:port} format.
     *
     * @return the leader address
     */
    public String getLeaderAddress() {
        return leaderAddress;
    }

    /**
     * Returns the name of the shard channel served by this replica.
     *
     * @return the channel name
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the IDs of the query nodes that serve the shard channel.
     *
     * @return the query node IDs
     */
    public List<Long> getNodeIDs() {
        return nodeIDs;
    }

    // Setters
    /**
     * Sets the ID of the leader query node of the shard replica.
     *
     * @param leaderID the leader query node ID
     */
    public void setLeaderID(Long leaderID) {
        this.leaderID = leaderID;
    }

    /**
     * Sets the address of the leader query node, in {@code IP:port} format.
     *
     * @param leaderAddress the leader address
     */
    public void setLeaderAddress(String leaderAddress) {
        this.leaderAddress = leaderAddress;
    }

    /**
     * Sets the name of the shard channel served by this replica.
     *
     * @param channelName the channel name
     */
    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    /**
     * Sets the IDs of the query nodes that serve the shard channel.
     *
     * @param nodeIDs the query node IDs
     */
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

    /**
     * Builder for {@link ShardReplica}.
     */
    public static class ShardReplicaBuilder {
        private Long leaderID;
        private String leaderAddress;
        private String channelName = "";
        private List<Long> nodeIDs = new ArrayList<>();

        /**
         * Sets the ID of the leader query node of the shard replica.
         *
         * @param leaderID the leader query node ID
         * @return this builder
         */
        public ShardReplicaBuilder leaderID(Long leaderID) {
            this.leaderID = leaderID;
            return this;
        }

        /**
         * Sets the address of the leader query node, in {@code IP:port} format.
         *
         * @param leaderAddress the leader address
         * @return this builder
         */
        public ShardReplicaBuilder leaderAddress(String leaderAddress) {
            this.leaderAddress = leaderAddress;
            return this;
        }

        /**
         * Sets the name of the shard channel served by this replica.
         *
         * @param channelName the channel name
         * @return this builder
         */
        public ShardReplicaBuilder channelName(String channelName) {
            this.channelName = channelName;
            return this;
        }

        /**
         * Sets the IDs of the query nodes that serve the shard channel.
         *
         * @param nodeIDs the query node IDs
         * @return this builder
         */
        public ShardReplicaBuilder nodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
            return this;
        }

        /**
         * Builds the {@link ShardReplica}.
         *
         * @return the shard replica
         */
        public ShardReplica build() {
            return new ShardReplica(this);
        }
    }
}
