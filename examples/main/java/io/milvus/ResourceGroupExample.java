package io.milvus;

import com.google.gson.Gson;

import io.milvus.client.MilvusServiceClient;
import io.milvus.resourcegroup.ResourceGroupManagement;
import io.milvus.param.ConnectParam;

public class ResourceGroupExample {
    private static final ResourceGroupManagement manager;
    private static final String rgName1 = "rg1";
    private static final String rgName2 = "rg2";
    private static Gson gson = new Gson();

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withAuthorization("root", "Milvus")
                .build();
        manager = new ResourceGroupManagement(new MilvusServiceClient(connectParam));
    }

    private static void printResourceGroupInfo() throws Exception {
        manager.listResourceGroups().forEach((name, rg) -> {
            System.out.println(name);
            System.out.println(gson.toJson(rg));
        });
    }

    public static void main(String[] args) throws Exception {
        // It's a demo to show how to use resource group management.
        // It create a database-level-resource-group-control (single replica)
        // management.
        // Declarative resource group API can also achieve
        // replica-level-resource-group-control (multi-replica) management.

        printResourceGroupInfo();
        // Initialize the cluster with 1 resource group
        // default_rg: 1
        manager.initializeCluster(1);
        printResourceGroupInfo();

        // Add one more node to default rg.
        manager.scaleResourceGroupTo(ResourceGroupManagement.DEFAULT_RG, 2);
        // add new query node.
        // default_rg: 2
        printResourceGroupInfo();

        // Add a new resource group.
        manager.createResourceGroup(rgName1, 1);
        // default_rg: 2, rg1: 0
        // add new query node.
        // default_rg: 2, rg1: 1
        printResourceGroupInfo();

        // Add a new resource group.
        manager.createResourceGroup(rgName2, 2);
        // default_rg: 2, rg1: 1, rg2: 0
        // add new query node.
        // default_rg: 2, rg1: 1, rg2: 1
        // add new query node.
        // default_rg: 2, rg1: 1, rg2: 2
        printResourceGroupInfo();

        // downscale default_rg to 1
        manager.scaleResourceGroupTo(ResourceGroupManagement.DEFAULT_RG, 1);
        // default_rg: 1, rg1: 1, rg2: 2, recycle_rg: 1
        printResourceGroupInfo();

        manager.scaleResourceGroupTo(ResourceGroupManagement.DEFAULT_RG, 5);
        manager.scaleResourceGroupTo(rgName1, 1);
        manager.scaleResourceGroupTo(rgName2, 3);
        // keep 8 query node in cluster.
        printResourceGroupInfo();

        // if there are replicas in other rg, transfer them to default together.
        manager.transferDataBaseToResourceGroup("default", ResourceGroupManagement.DEFAULT_RG);
        printResourceGroupInfo();
    }
}