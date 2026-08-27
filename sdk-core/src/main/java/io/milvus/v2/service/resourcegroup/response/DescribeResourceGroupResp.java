package io.milvus.v2.service.resourcegroup.response;

import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response returned by the {@code describeResourceGroup} API.
 */
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

    /**
     * Creates a new builder for {@code DescribeResourceGroupResp}.
     *
     * @return the builder
     */
    public static DescribeResourceGroupRespBuilder builder() {
        return new DescribeResourceGroupRespBuilder();
    }

    /**
     * Returns the name of the resource group.
     *
     * @return the resource group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the name of the resource group.
     *
     * @param groupName the resource group name
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Returns the number of query nodes in the resource group.
     *
     * @return the capacity of the resource group
     */
    public Integer getCapacity() {
        return capacity;
    }

    /**
     * Sets the number of query nodes in the resource group.
     *
     * @param capacity the capacity of the resource group
     */
    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    /**
     * Returns the number of available query nodes in the resource group.
     *
     * @return the number of available nodes
     */
    public Integer getNumberOfAvailableNode() {
        return numberOfAvailableNode;
    }

    /**
     * Sets the number of available query nodes in the resource group.
     *
     * @param numberOfAvailableNode the number of available nodes
     */
    public void setNumberOfAvailableNode(Integer numberOfAvailableNode) {
        this.numberOfAvailableNode = numberOfAvailableNode;
    }

    /**
     * Returns the number of loaded replicas for each collection in the resource group.
     *
     * @return a map of collection name to the number of loaded replicas
     */
    public Map<String, Integer> getNumberOfLoadedReplica() {
        return numberOfLoadedReplica;
    }

    /**
     * Sets the number of loaded replicas for each collection in the resource group.
     *
     * @param numberOfLoadedReplica a map of collection name to the number of loaded replicas
     */
    public void setNumberOfLoadedReplica(Map<String, Integer> numberOfLoadedReplica) {
        this.numberOfLoadedReplica = numberOfLoadedReplica;
    }

    /**
     * Returns the number of nodes being transferred out to other resource groups.
     *
     * @return a map of target resource group name to the number of outgoing nodes
     */
    public Map<String, Integer> getNumberOfOutgoingNode() {
        return numberOfOutgoingNode;
    }

    /**
     * Sets the number of nodes being transferred out to other resource groups.
     *
     * @param numberOfOutgoingNode a map of target resource group name to the number of outgoing nodes
     */
    public void setNumberOfOutgoingNode(Map<String, Integer> numberOfOutgoingNode) {
        this.numberOfOutgoingNode = numberOfOutgoingNode;
    }

    /**
     * Returns the number of nodes being transferred in from other resource groups.
     *
     * @return a map of source resource group name to the number of incoming nodes
     */
    public Map<String, Integer> getNumberOfIncomingNode() {
        return numberOfIncomingNode;
    }

    /**
     * Sets the number of nodes being transferred in from other resource groups.
     *
     * @param numberOfIncomingNode a map of source resource group name to the number of incoming nodes
     */
    public void setNumberOfIncomingNode(Map<String, Integer> numberOfIncomingNode) {
        this.numberOfIncomingNode = numberOfIncomingNode;
    }

    /**
     * Returns the configuration of the resource group.
     *
     * @return the resource group config
     */
    public ResourceGroupConfig getConfig() {
        return config;
    }

    /**
     * Sets the configuration of the resource group.
     *
     * @param config the resource group config
     */
    public void setConfig(ResourceGroupConfig config) {
        this.config = config;
    }

    /**
     * Returns the query nodes belonging to the resource group.
     *
     * @return the list of node information
     */
    public List<NodeInfo> getNodes() {
        return nodes;
    }

    /**
     * Sets the query nodes belonging to the resource group.
     *
     * @param nodes the list of node information
     */
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

        /**
         * Sets the name of the resource group.
         *
         * @param groupName the resource group name
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        /**
         * Sets the number of query nodes in the resource group.
         *
         * @param capacity the capacity of the resource group
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder capacity(Integer capacity) {
            this.capacity = capacity;
            return this;
        }

        /**
         * Sets the number of available query nodes in the resource group.
         *
         * @param numberOfAvailableNode the number of available nodes
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder numberOfAvailableNode(Integer numberOfAvailableNode) {
            this.numberOfAvailableNode = numberOfAvailableNode;
            return this;
        }

        /**
         * Sets the number of loaded replicas for each collection in the resource group.
         *
         * @param numberOfLoadedReplica a map of collection name to the number of loaded replicas
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder numberOfLoadedReplica(Map<String, Integer> numberOfLoadedReplica) {
            this.numberOfLoadedReplica = numberOfLoadedReplica;
            return this;
        }

        /**
         * Sets the number of nodes being transferred out to other resource groups.
         *
         * @param numberOfOutgoingNode a map of target resource group name to the number of outgoing nodes
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder numberOfOutgoingNode(Map<String, Integer> numberOfOutgoingNode) {
            this.numberOfOutgoingNode = numberOfOutgoingNode;
            return this;
        }

        /**
         * Sets the number of nodes being transferred in from other resource groups.
         *
         * @param numberOfIncomingNode a map of source resource group name to the number of incoming nodes
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder numberOfIncomingNode(Map<String, Integer> numberOfIncomingNode) {
            this.numberOfIncomingNode = numberOfIncomingNode;
            return this;
        }

        /**
         * Sets the configuration of the resource group.
         *
         * @param config the resource group config
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Sets the query nodes belonging to the resource group.
         *
         * @param nodes the list of node information
         * @return this builder
         */
        public DescribeResourceGroupRespBuilder nodes(List<NodeInfo> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * Builds the {@code DescribeResourceGroupResp}.
         *
         * @return the built response
         */
        public DescribeResourceGroupResp build() {
            return new DescribeResourceGroupResp(this);
        }
    }
}
