package io.milvus.common.resourcegroup;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroupTransfer {
    private String resourceGroupName;

    /**
     * Constructor with resource group name.
     * 
     * @param resourceGroupName
     */
    public ResourceGroupTransfer(@NonNull String resourceGroupName) {
        this.resourceGroupName = resourceGroupName;
    }

    /**
     * Constructor from grpc
     * 
     * @param grpcTransfer
     */
    public ResourceGroupTransfer(@NonNull io.milvus.grpc.ResourceGroupTransfer grpcTransfer) {
        this.resourceGroupName = grpcTransfer.getResourceGroup();
    }

    /**
     * Transfer to grpc
     * 
     * @return io.milvus.grpc.ResourceGroupTransfer
     */
    public @NonNull io.milvus.grpc.ResourceGroupTransfer toGRPC() {
        return io.milvus.grpc.ResourceGroupTransfer.newBuilder()
                .setResourceGroup(resourceGroupName)
                .build();
    }
}
