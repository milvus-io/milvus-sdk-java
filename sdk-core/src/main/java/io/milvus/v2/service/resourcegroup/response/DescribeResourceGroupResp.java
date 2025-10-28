package io.milvus.v2.service.resourcegroup.response;

import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeResourceGroupResp {
    private String groupName;
    private Integer capacity;
    private Integer numberOfAvailableNode;
    private Map<String, Integer> numberOfLoadedReplica;
    private Map<String, Integer> numberOfOutgoingNode;
    private Map<String, Integer> numberOfIncomingNode;
    private ResourceGroupConfig config;
    private List<NodeInfo> nodes;

    private DescribeResourceGroupResp(DescribeResourceGroupRespBuilder builder) {
        this.groupName = builder.groupName;
        this.capacity = builder.capacity;
        this.numberOfAvailableNode = builder.numberOfAvailableNode;
        this.numberOfLoadedReplica = builder.numberOfLoadedReplica;
        this.numberOfOutgoingNode = builder.numberOfOutgoingNode;
        this.numberOfIncomingNode = builder.numberOfIncomingNode;
        this.config = builder.config;
        this.nodes = builder.nodes;
    }

    public static DescribeResourceGroupRespBuilder builder() {
        return new DescribeResourceGroupRespBuilder();
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

    public static class DescribeResourceGroupRespBuilder {
        private String groupName;
        private Integer capacity;
        private Integer numberOfAvailableNode;
        private ResourceGroupConfig config;
        private Map<String, Integer> numberOfLoadedReplica = new HashMap<>();
        private Map<String, Integer> numberOfOutgoingNode = new HashMap<>();
        private Map<String, Integer> numberOfIncomingNode = new HashMap<>();
        private List<NodeInfo> nodes = new ArrayList<>();

        public DescribeResourceGroupRespBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public DescribeResourceGroupRespBuilder capacity(Integer capacity) {
            this.capacity = capacity;
            return this;
        }

        public DescribeResourceGroupRespBuilder numberOfAvailableNode(Integer numberOfAvailableNode) {
            this.numberOfAvailableNode = numberOfAvailableNode;
            return this;
        }

        public DescribeResourceGroupRespBuilder numberOfLoadedReplica(Map<String, Integer> numberOfLoadedReplica) {
            this.numberOfLoadedReplica = numberOfLoadedReplica;
            return this;
        }

        public DescribeResourceGroupRespBuilder numberOfOutgoingNode(Map<String, Integer> numberOfOutgoingNode) {
            this.numberOfOutgoingNode = numberOfOutgoingNode;
            return this;
        }

        public DescribeResourceGroupRespBuilder numberOfIncomingNode(Map<String, Integer> numberOfIncomingNode) {
            this.numberOfIncomingNode = numberOfIncomingNode;
            return this;
        }

        public DescribeResourceGroupRespBuilder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        public DescribeResourceGroupRespBuilder nodes(List<NodeInfo> nodes) {
            this.nodes = nodes;
            return this;
        }

        public DescribeResourceGroupResp build() {
            return new DescribeResourceGroupResp(this);
        }
    }
}
