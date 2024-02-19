package io.milvus.common.resourcegroup;

import org.jetbrains.annotations.NotNull;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroupLimit {
    private Integer nodeNum;

    /**
     * Constructor with node number.
     * 
     * @param nodeNum
     */
    public ResourceGroupLimit(@NonNull Integer nodeNum) {
        this.nodeNum = nodeNum;
    }

    /**
     * Constructor from grpc
     * 
     * @param grpcLimit
     */
    public ResourceGroupLimit(@NonNull io.milvus.grpc.ResourceGroupLimit grpcLimit) {
        this.nodeNum = grpcLimit.getNodeNum();
    }

    /**
     * Transfer to grpc
     * 
     * @return io.milvus.grpc.ResourceGroupLimit
     */
    public @NonNull io.milvus.grpc.ResourceGroupLimit toGRPC() {
        return io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(nodeNum).build();
    }
}
