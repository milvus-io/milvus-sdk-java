package io.milvus.common.resourcegroup;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroup {
    private String resourceGroupName;
    private Integer availableNodeNum;
    private java.util.Map<String, Integer> loadedReplicaNum;
    private java.util.Map<String, Integer> outgoingNodeNum;
    private java.util.Map<String, Integer> incomingNodeNum;
    private ResourceGroupConfig resourceGroupConfig;

    public ResourceGroup(@NonNull io.milvus.grpc.ResourceGroup rg) {
        this.resourceGroupName = rg.getName();
        this.availableNodeNum = rg.getNumAvailableNode();
        this.loadedReplicaNum = rg.getNumLoadedReplicaMap();
        this.outgoingNodeNum = rg.getNumOutgoingNodeMap();
        this.incomingNodeNum = rg.getNumIncomingNodeMap();
        this.resourceGroupConfig = new ResourceGroupConfig(rg.getConfig());
    }
}
