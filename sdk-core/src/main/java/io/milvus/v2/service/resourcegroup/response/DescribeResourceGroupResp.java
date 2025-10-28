package io.milvus.v2.service.resourcegroup.response;

import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

public class DescribeResourceGroupResp {
    private String groupName;
    private Integer capacity;
    private Integer numberOfAvailableNode;
    private Map<String, Integer> numberOfLoadedReplica;
    private Map<String, Integer> numberOfOutgoingNode;
    private Map<String, Integer> numberOfIncomingNode;
    private ResourceGroupConfig config;
    private List<NodeInfo> nodes;

    private DescribeResourceGroupResp(Builder builder) {
        this.groupName = builder.groupName;
        this.capacity = builder.capacity;
        this.numberOfAvailableNode = builder.numberOfAvailableNode;
        this.numberOfLoadedReplica = builder.numberOfLoadedReplica;
        this.numberOfOutgoingNode = builder.numberOfOutgoingNode;
        this.numberOfIncomingNode = builder.numberOfIncomingNode;
        this.config = builder.config;
        this.nodes = builder.nodes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public Integer getNumberOfAvailableNode() {
        return numberOfAvailableNode;
    }

    public void setNumberOfAvailableNode(Integer numberOfAvailableNode) {
        this.numberOfAvailableNode = numberOfAvailableNode;
    }

    public Map<String, Integer> getNumberOfLoadedReplica() {
        return numberOfLoadedReplica;
    }

    public void setNumberOfLoadedReplica(Map<String, Integer> numberOfLoadedReplica) {
        this.numberOfLoadedReplica = numberOfLoadedReplica;
    }

    public Map<String, Integer> getNumberOfOutgoingNode() {
        return numberOfOutgoingNode;
    }

    public void setNumberOfOutgoingNode(Map<String, Integer> numberOfOutgoingNode) {
        this.numberOfOutgoingNode = numberOfOutgoingNode;
    }

    public Map<String, Integer> getNumberOfIncomingNode() {
        return numberOfIncomingNode;
    }

    public void setNumberOfIncomingNode(Map<String, Integer> numberOfIncomingNode) {
        this.numberOfIncomingNode = numberOfIncomingNode;
    }

    public ResourceGroupConfig getConfig() {
        return config;
    }

    public void setConfig(ResourceGroupConfig config) {
        this.config = config;
    }

    public List<NodeInfo> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeResourceGroupResp that = (DescribeResourceGroupResp) obj;
        return new EqualsBuilder()
                .append(groupName, that.groupName)
                .append(capacity, that.capacity)
                .append(numberOfAvailableNode, that.numberOfAvailableNode)
                .append(numberOfLoadedReplica, that.numberOfLoadedReplica)
                .append(numberOfOutgoingNode, that.numberOfOutgoingNode)
                .append(numberOfIncomingNode, that.numberOfIncomingNode)
                .append(config, that.config)
                .append(nodes, that.nodes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(groupName)
                .append(capacity)
                .append(numberOfAvailableNode)
                .append(numberOfLoadedReplica)
                .append(numberOfOutgoingNode)
                .append(numberOfIncomingNode)
                .append(config)
                .append(nodes)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "DescribeResourceGroupResp{" +
                "groupName='" + groupName + '\'' +
                ", capacity=" + capacity +
                ", numberOfAvailableNode=" + numberOfAvailableNode +
                ", numberOfLoadedReplica=" + numberOfLoadedReplica +
                ", numberOfOutgoingNode=" + numberOfOutgoingNode +
                ", numberOfIncomingNode=" + numberOfIncomingNode +
                ", config=" + config +
                ", nodes=" + nodes +
                '}';
    }

    public static class Builder {
        private String groupName;
        private Integer capacity;
        private Integer numberOfAvailableNode;
        private ResourceGroupConfig config;
        private Map<String, Integer> numberOfLoadedReplica = new HashMap<>();
        private Map<String, Integer> numberOfOutgoingNode = new HashMap<>();
        private Map<String, Integer> numberOfIncomingNode = new HashMap<>();
        private List<NodeInfo> nodes = new ArrayList<>();

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder capacity(Integer capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder numberOfAvailableNode(Integer numberOfAvailableNode) {
            this.numberOfAvailableNode = numberOfAvailableNode;
            return this;
        }

        public Builder numberOfLoadedReplica(Map<String, Integer> numberOfLoadedReplica) {
            this.numberOfLoadedReplica = numberOfLoadedReplica;
            return this;
        }

        public Builder numberOfOutgoingNode(Map<String, Integer> numberOfOutgoingNode) {
            this.numberOfOutgoingNode = numberOfOutgoingNode;
            return this;
        }

        public Builder numberOfIncomingNode(Map<String, Integer> numberOfIncomingNode) {
            this.numberOfIncomingNode = numberOfIncomingNode;
            return this;
        }

        public Builder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        public Builder nodes(List<NodeInfo> nodes) {
            this.nodes = nodes;
            return this;
        }

        public DescribeResourceGroupResp build() {
            return new DescribeResourceGroupResp(this);
        }
    }
}
