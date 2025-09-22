package io.milvus.v2.service.resourcegroup;

import io.milvus.grpc.*;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.resourcegroup.request.*;
import io.milvus.v2.service.resourcegroup.response.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResourceGroupService extends BaseService {
    private static ResourceGroupConfig convertResourceGroupConfig(io.milvus.common.resourcegroup.ResourceGroupConfig config) {
        if (config == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid resource group config");
        }

        ResourceGroupConfig.Builder builder = ResourceGroupConfig.newBuilder();
        builder.setRequests(ResourceGroupLimit.newBuilder()
                .setNodeNum(config.getRequests().getNodeNum()))
                .build();
        builder.setLimits(ResourceGroupLimit.newBuilder()
                .setNodeNum(config.getLimits().getNodeNum()))
                .build();

        for (io.milvus.common.resourcegroup.ResourceGroupTransfer groupFrom : config.getFrom()) {
            builder.addTransferFrom(ResourceGroupTransfer.newBuilder()
                    .setResourceGroup(groupFrom.getResourceGroupName()))
                    .build();
        }

        for (io.milvus.common.resourcegroup.ResourceGroupTransfer groupTo : config.getTo()) {
            builder.addTransferTo(ResourceGroupTransfer.newBuilder()
                    .setResourceGroup(groupTo.getResourceGroupName()))
                    .build();
        }

        if (config.getNodeFilter() != null) {
            builder.setNodeFilter(config.getNodeFilter().toGRPC());
        }

        return builder.build();
    }

    private static io.milvus.common.resourcegroup.ResourceGroupConfig convertResourceGroupConfig(ResourceGroupConfig config) {
        List<io.milvus.common.resourcegroup.ResourceGroupTransfer> fromList = new ArrayList<>();
        config.getTransferFromList().forEach((groupFrom)->{
            fromList.add(new io.milvus.common.resourcegroup.ResourceGroupTransfer(groupFrom.getResourceGroup()));
        });

        List<io.milvus.common.resourcegroup.ResourceGroupTransfer> toList = new ArrayList<>();
        config.getTransferToList().forEach((groupTo)->{
            toList.add(new io.milvus.common.resourcegroup.ResourceGroupTransfer(groupTo.getResourceGroup()));
        });

        return io.milvus.common.resourcegroup.ResourceGroupConfig.newBuilder()
                .withRequests(new io.milvus.common.resourcegroup.ResourceGroupLimit(config.getRequests().getNodeNum()))
                .withLimits(new io.milvus.common.resourcegroup.ResourceGroupLimit(config.getLimits().getNodeNum()))
                .withFrom(fromList)
                .withTo(toList)
                .withNodeFilter(new io.milvus.common.resourcegroup.ResourceGroupNodeFilter(config.getNodeFilter()))
                .build();
    }

    public Void createResourceGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                    CreateResourceGroupReq request) {
        String title = String.format("Create resource group: '%s'", request.getGroupName());

        ResourceGroupConfig rpcConfig = convertResourceGroupConfig(request.getConfig());
        CreateResourceGroupRequest rpcRequest = CreateResourceGroupRequest.newBuilder()
                .setResourceGroup(request.getGroupName())
                .setConfig(rpcConfig)
                .build();

        Status status = blockingStub.createResourceGroup(rpcRequest);
        rpcUtils.handleResponse(title, status);
        return null;
    }

    public Void updateResourceGroups(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                     UpdateResourceGroupsReq request) {
        Map<String, io.milvus.common.resourcegroup.ResourceGroupConfig> resourceGroups = request.getResourceGroups();
        if (resourceGroups.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Resource group configurations cannot be empty");
        }

        UpdateResourceGroupsRequest.Builder requestBuilder = UpdateResourceGroupsRequest.newBuilder();
        resourceGroups.forEach((groupName, config) -> {
            ResourceGroupConfig rpcConfig = convertResourceGroupConfig(config);
            requestBuilder.putResourceGroups(groupName, rpcConfig);
        });

        Status status = blockingStub.updateResourceGroups(requestBuilder.build());
        rpcUtils.handleResponse("Update resource groups", status);
        return null;
    }

    public Void dropResourceGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                  DropResourceGroupReq request) {
        String title = String.format("Drop resource group: '%s'", request.getGroupName());

        DropResourceGroupRequest rpcRequest = DropResourceGroupRequest.newBuilder()
                .setResourceGroup(request.getGroupName())
                .build();

        Status status = blockingStub.dropResourceGroup(rpcRequest);
        rpcUtils.handleResponse(title, status);
        return null;
    }

    public ListResourceGroupsResp listResourceGroups(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                     ListResourceGroupsReq request) {
        ListResourceGroupsResponse response = blockingStub.listResourceGroups(ListResourceGroupsRequest.newBuilder().build());
        rpcUtils.handleResponse("List resource groups", response.getStatus());
        return ListResourceGroupsResp.builder()
                .groupNames(response.getResourceGroupsList())
                .build();
    }

    public DescribeResourceGroupResp describeResourceGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                           DescribeResourceGroupReq request) {
        String title = String.format("Describe resource group: '%s'", request.getGroupName());
        DescribeResourceGroupRequest rpcRequest = DescribeResourceGroupRequest.newBuilder()
                .setResourceGroup(request.getGroupName())
                .build();

        DescribeResourceGroupResponse response = blockingStub.describeResourceGroup(rpcRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        ResourceGroup rgroup = response.getResourceGroup();
        List<io.milvus.common.resourcegroup.NodeInfo> nodes = new ArrayList<>();
        rgroup.getNodesList().forEach((node)->{
            nodes.add(io.milvus.common.resourcegroup.NodeInfo.builder()
                    .nodeId(node.getNodeId())
                    .address(node.getAddress())
                    .hostname(node.getHostname())
                    .build());
        });
        return DescribeResourceGroupResp.builder()
                .groupName(rgroup.getName())
                .capacity(rgroup.getCapacity())
                .numberOfAvailableNode(rgroup.getNumAvailableNode())
                .numberOfLoadedReplica(rgroup.getNumLoadedReplicaMap())
                .numberOfOutgoingNode(rgroup.getNumOutgoingNodeMap())
                .numberOfIncomingNode(rgroup.getNumIncomingNodeMap())
                .config(convertResourceGroupConfig(rgroup.getConfig()))
                .nodes(nodes)
                .build();
    }

    public Void transferNode(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, TransferNodeReq request) {
        String sourceGroup = request.getSourceGroupName();
        if (StringUtils.isEmpty(sourceGroup)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid source group name");
        }
        String targetGroup = request.getTargetGroupName();
        if (StringUtils.isEmpty(targetGroup)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid target group name");
        }

        Integer numOfNodes = request.getNumOfNodes();
        String title = String.format("Transfer %d nodes from group: '%s' to group: '%s'", numOfNodes, sourceGroup, targetGroup);
        Status response = blockingStub.transferNode(TransferNodeRequest.newBuilder()
                .setSourceResourceGroup(sourceGroup)
                .setTargetResourceGroup(targetGroup)
                .setNumNode(numOfNodes)
                .build());
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public Void transferReplica(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                TransferReplicaReq request) {
        String sourceGroup = request.getSourceGroupName();
        if (StringUtils.isEmpty(sourceGroup)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid source group name");
        }
        String targetGroup = request.getTargetGroupName();
        if (StringUtils.isEmpty(targetGroup)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid target group name");
        }
        String collectionName = request.getCollectionName();
        if (StringUtils.isEmpty(collectionName)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid collection name");
        }

        String dbName = request.getDatabaseName();
        Long numOfReplicas = request.getNumberOfReplicas();
        String title = String.format("Transfer %d replicas from group: '%s' to group: '%s' of collection: '%s'",
                numOfReplicas, sourceGroup, targetGroup, collectionName);

        TransferReplicaRequest.Builder requestBuilder = TransferReplicaRequest.newBuilder()
                .setSourceResourceGroup(sourceGroup)
                .setTargetResourceGroup(targetGroup)
                .setCollectionName(collectionName)
                .setNumReplica(numOfReplicas);

        if (StringUtils.isNotEmpty(dbName)) {
            requestBuilder.setDbName(dbName);
        }

        Status status = blockingStub.transferReplica(requestBuilder.build());
        rpcUtils.handleResponse(title, status);
        return null;
    }
}
