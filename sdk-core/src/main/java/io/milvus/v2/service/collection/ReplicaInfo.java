package io.milvus.v2.service.collection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Information of a replica of a loaded collection, as returned by the {@code describeReplicas} API.
 */
public class ReplicaInfo {
    private Long replicaID;
    private Long collectionID;
    private List<Long> partitionIDs;
    private List<ShardReplica> shardReplicas;
    private List<Long> nodeIDs; // include leaders
    private String resourceGroupName;
    private Map<String, Integer> numOutboundNode;

    private ReplicaInfo(ReplicaInfoBuilder builder) {
        this.replicaID = builder.replicaID;
        this.collectionID = builder.collectionID;
        this.partitionIDs = builder.partitionIDs != null ? builder.partitionIDs : new ArrayList<>();
        this.shardReplicas = builder.shardReplicas != null ? builder.shardReplicas : new ArrayList<>();
        this.nodeIDs = builder.nodeIDs != null ? builder.nodeIDs : new ArrayList<>();
        this.resourceGroupName = builder.resourceGroupName != null ? builder.resourceGroupName : "";
        this.numOutboundNode = builder.numOutboundNode != null ? builder.numOutboundNode : new HashMap<>();
    }

    /**
     * Creates a new {@code ReplicaInfo} builder.
     *
     * @return the builder
     */
    public static ReplicaInfoBuilder builder() {
        return new ReplicaInfoBuilder();
    }

    // Getters
    /**
     * Returns the ID of the replica.
     *
     * @return the replica ID
     */
    public Long getReplicaID() {
        return replicaID;
    }

    /**
     * Returns the ID of the collection to which the replica belongs.
     *
     * @return the collection ID
     */
    public Long getCollectionID() {
        return collectionID;
    }

    /**
     * Returns the IDs of the partitions loaded on the replica.
     *
     * @return the partition IDs
     */
    public List<Long> getPartitionIDs() {
        return partitionIDs;
    }

    /**
     * Returns the shard replicas that serve the shard channels of the collection.
     *
     * @return the shard replicas
     */
    public List<ShardReplica> getShardReplicas() {
        return shardReplicas;
    }

    /**
     * Returns the IDs of the query nodes that host the replica, including the leaders.
     *
     * @return the query node IDs
     */
    public List<Long> getNodeIDs() {
        return nodeIDs;
    }

    /**
     * Returns the name of the resource group to which the replica belongs.
     *
     * @return the resource group name
     */
    public String getResourceGroupName() {
        return resourceGroupName;
    }

    /**
     * Returns the number of outbound query nodes contributed by each resource group.
     *
     * @return a map of resource group name to the number of outbound nodes
     */
    public Map<String, Integer> getNumOutboundNode() {
        return numOutboundNode;
    }

    // Setters
    /**
     * Sets the ID of the replica.
     *
     * @param replicaID the replica ID
     */
    public void setReplicaID(Long replicaID) {
        this.replicaID = replicaID;
    }

    /**
     * Sets the ID of the collection to which the replica belongs.
     *
     * @param collectionID the collection ID
     */
    public void setCollectionID(Long collectionID) {
        this.collectionID = collectionID;
    }

    /**
     * Sets the IDs of the partitions loaded on the replica.
     *
     * @param partitionIDs the partition IDs
     */
    public void setPartitionIDs(List<Long> partitionIDs) {
        this.partitionIDs = partitionIDs;
    }

    /**
     * Sets the shard replicas that serve the shard channels of the collection.
     *
     * @param shardReplicas the shard replicas
     */
    public void setShardReplicas(List<ShardReplica> shardReplicas) {
        this.shardReplicas = shardReplicas;
    }

    /**
     * Sets the IDs of the query nodes that host the replica.
     *
     * @param nodeIDs the query node IDs
     */
    public void setNodeIDs(List<Long> nodeIDs) {
        this.nodeIDs = nodeIDs;
    }

    /**
     * Sets the name of the resource group to which the replica belongs.
     *
     * @param resourceGroupName the resource group name
     */
    public void setResourceGroupName(String resourceGroupName) {
        this.resourceGroupName = resourceGroupName;
    }

    /**
     * Sets the number of outbound query nodes contributed by each resource group.
     *
     * @param numOutboundNode a map of resource group name to the number of outbound nodes
     */
    public void setNumOutboundNode(Map<String, Integer> numOutboundNode) {
        this.numOutboundNode = numOutboundNode;
    }

    @Override
    public String toString() {
        return "ReplicaInfo{" +
                "replicaID=" + replicaID +
                ", collectionID=" + collectionID +
                ", partitionIDs=" + partitionIDs +
                ", shardReplicas=" + shardReplicas +
                ", nodeIDs=" + nodeIDs +
                ", resourceGroupName='" + resourceGroupName + '\'' +
                ", numOutboundNode=" + numOutboundNode +
                '}';
    }

    /**
     * Builder for {@link ReplicaInfo}.
     */
    public static class ReplicaInfoBuilder {
        private Long replicaID;
        private Long collectionID;
        private List<Long> partitionIDs;
        private List<ShardReplica> shardReplicas;
        private List<Long> nodeIDs;
        private String resourceGroupName;
        private Map<String, Integer> numOutboundNode;

        /**
         * Sets the ID of the replica.
         *
         * @param replicaID the replica ID
         * @return this builder
         */
        public ReplicaInfoBuilder replicaID(Long replicaID) {
            this.replicaID = replicaID;
            return this;
        }

        /**
         * Sets the ID of the collection to which the replica belongs.
         *
         * @param collectionID the collection ID
         * @return this builder
         */
        public ReplicaInfoBuilder collectionID(Long collectionID) {
            this.collectionID = collectionID;
            return this;
        }

        /**
         * Sets the IDs of the partitions loaded on the replica.
         *
         * @param partitionIDs the partition IDs
         * @return this builder
         */
        public ReplicaInfoBuilder partitionIDs(List<Long> partitionIDs) {
            this.partitionIDs = partitionIDs;
            return this;
        }

        /**
         * Sets the shard replicas that serve the shard channels of the collection.
         *
         * @param shardReplicas the shard replicas
         * @return this builder
         */
        public ReplicaInfoBuilder shardReplicas(List<ShardReplica> shardReplicas) {
            this.shardReplicas = shardReplicas;
            return this;
        }

        /**
         * Sets the IDs of the query nodes that host the replica.
         *
         * @param nodeIDs the query node IDs
         * @return this builder
         */
        public ReplicaInfoBuilder nodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
            return this;
        }

        /**
         * Sets the name of the resource group to which the replica belongs.
         *
         * @param resourceGroupName the resource group name
         * @return this builder
         */
        public ReplicaInfoBuilder resourceGroupName(String resourceGroupName) {
            this.resourceGroupName = resourceGroupName;
            return this;
        }

        /**
         * Sets the number of outbound query nodes contributed by each resource group.
         *
         * @param numOutboundNode a map of resource group name to the number of outbound nodes
         * @return this builder
         */
        public ReplicaInfoBuilder numOutboundNode(Map<String, Integer> numOutboundNode) {
            this.numOutboundNode = numOutboundNode;
            return this;
        }

        /**
         * Builds the {@link ReplicaInfo}.
         *
         * @return the replica information
         */
        public ReplicaInfo build() {
            return new ReplicaInfo(this);
        }
    }
}
