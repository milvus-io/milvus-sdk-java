package io.milvus.v2.service.resourcegroup.response;

import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.*;

@Data
@SuperBuilder
public class DescribeResourceGroupResp {
    private String groupName;
    private Integer capacity;
    private Integer numberOfAvailableNode;
    @Builder.Default
    private Map<String, Integer> numberOfLoadedReplica = new HashMap<>();
    @Builder.Default
    private Map<String, Integer> numberOfOutgoingNode = new HashMap<>();
    @Builder.Default
    private Map<String, Integer> numberOfIncomingNode = new HashMap<>();
    private ResourceGroupConfig config;
    @Builder.Default
    private List<NodeInfo> nodes = new ArrayList<>();
}
