package io.milvus.resourcegroup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.milvus.client.MilvusClient;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.GetLoadStateParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.control.GetReplicasParam;
import io.milvus.param.resourcegroup.CreateResourceGroupParam;
import io.milvus.param.resourcegroup.DescribeResourceGroupParam;
import io.milvus.param.resourcegroup.DropResourceGroupParam;
import io.milvus.param.resourcegroup.ListResourceGroupsParam;
import io.milvus.param.resourcegroup.TransferReplicaParam;
import io.milvus.param.resourcegroup.UpdateResourceGroupsParam;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.common.resourcegroup.ResourceGroupTransfer;
import io.milvus.exception.MilvusException;
import io.milvus.grpc.DescribeResourceGroupResponse;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.GetReplicasResponse;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.ListResourceGroupsResponse;
import io.milvus.grpc.LoadState;
import io.milvus.grpc.ShowCollectionsResponse;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ResourceGroupManagement {
    public static String RECYCLE_RG = "__recycle_resource_group";
    public static String DEFAULT_RG = "__default_resource_group";
    static Integer RECYCLE_RG_REQUEST_NODE_NUM = 0;
    static Integer RECYCLE_RG_LIMIT_NODE_NUM = 100000;

    protected static final Logger logger = LoggerFactory.getLogger(ResourceGroupManagement.class);

    private MilvusClient client;

    public ResourceGroupManagement(MilvusClient client) {
        this.client = client;
    }

    /**
     * list all resource groups.
     * 
     * @return map of resource group name and resource group info.
     */
    public Map<String, ResourceGroupInfo> listResourceGroups() throws Exception {
        // List all resource groups.
        R<ListResourceGroupsResponse> response = client
                .listResourceGroups(ListResourceGroupsParam.newBuilder().build());
        ListResourceGroupsResponse resourceGroups = unwrap(response);

        // Describe all resource groups.
        Map<String, ResourceGroupInfo.Builder> result = new HashMap<>();
        for (String resourceGroupName : resourceGroups.getResourceGroupsList()) {
            R<DescribeResourceGroupResponse> resourceGroupInfoResp = client.describeResourceGroup(
                    DescribeResourceGroupParam.newBuilder().withGroupName(resourceGroupName).build());
            DescribeResourceGroupResponse resourceGroupInfo = unwrap(resourceGroupInfoResp);

            ResourceGroupInfo.Builder builder = ResourceGroupInfo.newBuilder()
                    .withResourceGroupName(resourceGroupName)
                    .withConfig(new ResourceGroupConfig(resourceGroupInfo.getResourceGroup().getConfig()));

            resourceGroupInfo.getResourceGroup().getNodesList().forEach(node -> {
                builder.addAvailableNode(NodeInfo.newBuilder()
                        .withNodeId(node.getNodeId())
                        .withAddress(node.getAddress())
                        .withHostname(node.getHostname())
                        .build());
            });
            result.put(resourceGroupName, builder);
        }

        // Get map info between resource group and database.
        R<ListDatabasesResponse> listDatabaseGroupsResp = client.listDatabases();
        ListDatabasesResponse databases = unwrap(listDatabaseGroupsResp);
        // list all collections.
        for (String dbName : databases.getDbNamesList()) {
            R<ShowCollectionsResponse> resp = client.showCollections(ShowCollectionsParam.newBuilder()
                    .withDatabaseName(dbName)
                    .build());
            ShowCollectionsResponse showCollectionResponse = unwrap(resp);

            Set<String> resourceGroupNames = new HashSet<>();
            for (String collection : showCollectionResponse.getCollectionNamesList()) {
                String resourceGroupName = getCollectionResourceGroupName(dbName, collection);
                if (resourceGroupName != null) {
                    resourceGroupNames.add(resourceGroupName);
                }
            }

            if (resourceGroupNames.size() == 0) {
                logger.info("no loaded collection in database {}", dbName);
                continue;
            } else if (resourceGroupNames.size() == 1) {
                // all loaded collection in one resource group.
                for (String resourceGroupName : resourceGroupNames) {
                    result.get(resourceGroupName).addFullDatabases(dbName);
                }
            } else {
                // loaded collection in same db in different resource group.
                for (String resourceGroupName : resourceGroupNames) {
                    result.get(resourceGroupName).addPartialDatabases(dbName);
                }
            }
        }

        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * Initialize the cluster with a recycle resource group.
     * 
     * @param defaultResourceGroupNodeNum The number of query nodes to initialize
     *                                    the default resource group.
     */
    public void initializeCluster(Integer defaultResourceGroupNodeNum) throws Exception {
        // Create a recycle resource group to hold all redundant query node.
        R<RpcStatus> resp = client.createResourceGroup(CreateResourceGroupParam.newBuilder()
                .withGroupName(RECYCLE_RG)
                .withConfig(ResourceGroupConfig.newBuilder()
                        .withRequests(new ResourceGroupLimit(RECYCLE_RG_REQUEST_NODE_NUM))
                        .withLimits(new ResourceGroupLimit(RECYCLE_RG_LIMIT_NODE_NUM))
                        .build())
                .build());
        unwrap(resp);
        this.scaleResourceGroupTo(DEFAULT_RG, defaultResourceGroupNodeNum);
    }

    /**
     * Create a resource group with given nodeNum.
     *
     * @param resourceGroupName
     * @param requestNodeNum
     */
    public void createResourceGroup(String resourceGroupName, Integer requestNodeNum) throws Exception {
        R<RpcStatus> resp = client.createResourceGroup(CreateResourceGroupParam.newBuilder()
                .withGroupName(resourceGroupName)
                .withConfig(ResourceGroupConfig.newBuilder()
                        .withRequests(new ResourceGroupLimit(requestNodeNum))
                        .withLimits(new ResourceGroupLimit(requestNodeNum))
                        .withFrom(Arrays.asList(new ResourceGroupTransfer(RECYCLE_RG)))
                        .withTo(Arrays.asList(new ResourceGroupTransfer(RECYCLE_RG)))
                        .build())
                .build());
        unwrap(resp);
    }

    /**
     * Drop a resource group, before drop resource group, you should scale the
     * resource group to 0 first.
     * 
     * @param resourceGroupName
     */
    public void dropResourceGroup(String resourceGroupName) throws Exception {
        R<RpcStatus> resp = client
                .dropResourceGroup(DropResourceGroupParam.newBuilder().withGroupName(resourceGroupName).build());
        unwrap(resp);
    }

    /**
     * Scale to the number of nodes in a resource group.
     * 
     * @param resourceGroupName
     * @param requestNodeNum
     */
    public void scaleResourceGroupTo(String resourceGroupName, Integer requestNodeNum) throws Exception {
        if (resourceGroupName == RECYCLE_RG) {
            throw new IllegalArgumentException("Cannot scale to recycle resource group");
        }
        // Update a resource group with given nodeNum.
        // Set the resource group transfer to recycle resource group by default.
        R<RpcStatus> resp = client
                .updateResourceGroups(UpdateResourceGroupsParam.newBuilder()
                        .putResourceGroup(resourceGroupName, ResourceGroupConfig.newBuilder()
                                .withRequests(new ResourceGroupLimit(requestNodeNum))
                                .withLimits(new ResourceGroupLimit(requestNodeNum))
                                .withFrom(Arrays.asList(new ResourceGroupTransfer(RECYCLE_RG)))
                                .withTo(Arrays.asList(new ResourceGroupTransfer(RECYCLE_RG)))
                                .build())
                        .build());
        unwrap(resp);
    }

    /**
     * Transfer a database to specified resource group.
     * Only support single replica now.
     * 
     * @param dbName            The name of the database to transfer.
     * @param resourceGroupName The name of the target resource group.
     */
    public void transferDataBaseToResourceGroup(String dbName, String resourceGroupName) throws Exception {
        R<ShowCollectionsResponse> resp = client.showCollections(ShowCollectionsParam.newBuilder()
                .withDatabaseName(dbName)
                .build());
        ShowCollectionsResponse showCollectionResponse = unwrap(resp);

        for (String collection : showCollectionResponse.getCollectionNamesList()) {
            String currentResourceGroupName = getCollectionResourceGroupName(dbName, collection);
            // skip if the collection is not loaded or is already added to resourceGroup.
            if (currentResourceGroupName == null || currentResourceGroupName == resourceGroupName) {
                continue;
            }
            R<RpcStatus> status = client.transferReplica(TransferReplicaParam.newBuilder()
                    .withDatabaseName(dbName)
                    .withCollectionName(collection)
                    .withSourceGroupName(currentResourceGroupName)
                    .withTargetGroupName(resourceGroupName)
                    .withReplicaNumber(1L)
                    .build());
            unwrap(status);
        }
    }

    /**
     * get the resource group name of the collection.
     * 
     * @param dbName
     * @param collection
     * @return
     * @throws Exception
     */
    private String getCollectionResourceGroupName(String dbName, String collection) throws Exception {
        R<GetLoadStateResponse> loaded = client.getLoadState(
                GetLoadStateParam.newBuilder().withDatabaseName(dbName).withCollectionName(collection).build());
        GetLoadStateResponse loadedState = unwrap(loaded);

        if (loadedState.getState() != LoadState.LoadStateLoaded) {
            logger.info("Collection {} @ Database {} is not loaded, state is {}, skip it", collection, dbName,
                    loaded.getData().getState());
            return null;
        }

        // Get the current resource group of the collection.
        R<GetReplicasResponse> replicaResp = client
                .getReplicas(
                        GetReplicasParam.newBuilder().withCollectionName(collection).withDatabaseName(dbName).build());
        GetReplicasResponse replicas = unwrap(replicaResp);

        if (replicas.getReplicasCount() > 1) {
            // Multi replica is supported with multiple resource group.
            // But current example only support single replica, so throw exception here.
            // You can modify the code to support multi replicas.
            throw new RuntimeException(String.format(
                    "Replica number {} is greater than 1, did not support now in current example, you can modify the code to support it.",
                    replicas.getReplicasCount()));
        }
        if (replicas.getReplicasCount() == 0) {
            logger.warn("Collection {} @ Database {} has no replica, skip it", collection, dbName);
            return null;
        }

        return replicas.getReplicasList().get(0).getResourceGroupName();
    }

    /**
     * 
     * @param <T>
     * @param response
     * @return
     */
    private <T> T unwrap(R<T> response) throws Exception {
        if (response.getStatus() != 0) {
            if (response.getException() instanceof MilvusException) {
                MilvusException e = (MilvusException) response.getException();
                throw e;
            }
            logger.warn("at unwrap", response.getException());
            throw response.getException();
        }
        return response.getData();
    }
}
