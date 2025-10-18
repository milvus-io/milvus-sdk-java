package io.milvus.v2.service.collection;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ReplicaInfo {
    private Long replicaID;
    private Long collectionID;
    private List<Long> partitionIDs;
    private List<ShardReplica> shardReplicas;
    private List<Long> nodeIDs; // include leaders
    private String resourceGroupName;
    private Map<String, Integer> numOutboundNode;

    private ReplicaInfo(Builder builder) {
        this.replicaID = builder.replicaID;
        this.collectionID = builder.collectionID;
        this.partitionIDs = builder.partitionIDs != null ? builder.partitionIDs : new ArrayList<>();
        this.shardReplicas = builder.shardReplicas != null ? builder.shardReplicas : new ArrayList<>();
        this.nodeIDs = builder.nodeIDs != null ? builder.nodeIDs : new ArrayList<>();
        this.resourceGroupName = builder.resourceGroupName != null ? builder.resourceGroupName : "";
        this.numOutboundNode = builder.numOutboundNode != null ? builder.numOutboundNode : new HashMap<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public Long getReplicaID() {
        return replicaID;
    }

    public Long getCollectionID() {
        return collectionID;
    }

    public List<Long> getPartitionIDs() {
        return partitionIDs;
    }

    public List<ShardReplica> getShardReplicas() {
        return shardReplicas;
    }

    public List<Long> getNodeIDs() {
        return nodeIDs;
    }

    public String getResourceGroupName() {
        return resourceGroupName;
    }

    public Map<String, Integer> getNumOutboundNode() {
        return numOutboundNode;
    }

    // Setters
    public void setReplicaID(Long replicaID) {
        this.replicaID = replicaID;
    }

    public void setCollectionID(Long collectionID) {
        this.collectionID = collectionID;
    }

    public void setPartitionIDs(List<Long> partitionIDs) {
        this.partitionIDs = partitionIDs;
    }

    public void setShardReplicas(List<ShardReplica> shardReplicas) {
        this.shardReplicas = shardReplicas;
    }

    public void setNodeIDs(List<Long> nodeIDs) {
        this.nodeIDs = nodeIDs;
    }

    public void setResourceGroupName(String resourceGroupName) {
        this.resourceGroupName = resourceGroupName;
    }

    public void setNumOutboundNode(Map<String, Integer> numOutboundNode) {
        this.numOutboundNode = numOutboundNode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ReplicaInfo that = (ReplicaInfo) obj;
        
        return new EqualsBuilder()
                .append(replicaID, that.replicaID)
                .append(collectionID, that.collectionID)
                .append(partitionIDs, that.partitionIDs)
                .append(shardReplicas, that.shardReplicas)
                .append(nodeIDs, that.nodeIDs)
                .append(resourceGroupName, that.resourceGroupName)
                .append(numOutboundNode, that.numOutboundNode)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicaID, collectionID, partitionIDs, shardReplicas, 
                nodeIDs, resourceGroupName, numOutboundNode);
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

    public static class Builder {
        private Long replicaID;
        private Long collectionID;
        private List<Long> partitionIDs;
        private List<ShardReplica> shardReplicas;
        private List<Long> nodeIDs;
        private String resourceGroupName;
        private Map<String, Integer> numOutboundNode;

        public Builder replicaID(Long replicaID) {
            this.replicaID = replicaID;
            return this;
        }

        public Builder collectionID(Long collectionID) {
            this.collectionID = collectionID;
            return this;
        }

        public Builder partitionIDs(List<Long> partitionIDs) {
            this.partitionIDs = partitionIDs;
            return this;
        }

        public Builder shardReplicas(List<ShardReplica> shardReplicas) {
            this.shardReplicas = shardReplicas;
            return this;
        }

        public Builder nodeIDs(List<Long> nodeIDs) {
            this.nodeIDs = nodeIDs;
            return this;
        }

        public Builder resourceGroupName(String resourceGroupName) {
            this.resourceGroupName = resourceGroupName;
            return this;
        }

        public Builder numOutboundNode(Map<String, Integer> numOutboundNode) {
            this.numOutboundNode = numOutboundNode;
            return this;
        }

        public ReplicaInfo build() {
            return new ReplicaInfo(this);
        }
    }
}
