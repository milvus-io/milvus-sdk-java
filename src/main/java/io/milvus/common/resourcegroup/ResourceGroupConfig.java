package io.milvus.common.resourcegroup;

import java.util.stream.Collectors;
import java.util.List;
import java.util.ArrayList;
import lombok.NonNull;
import lombok.Getter;

@Getter
public class ResourceGroupConfig {
    private final ResourceGroupLimit requests;
    private final ResourceGroupLimit limits;
    private final List<ResourceGroupTransfer> from;
    private final List<ResourceGroupTransfer> to;

    private ResourceGroupConfig(Builder builder) {
        this.requests = builder.requests;
        this.limits = builder.limits;

        if (null == builder.from) {
            this.from = new ArrayList<>();
        } else {
            this.from = builder.from;
        }

        if (null == builder.to) {
            this.to = new ArrayList<>();
        } else {
            this.to = builder.to;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private ResourceGroupLimit requests;
        private ResourceGroupLimit limits;
        private List<ResourceGroupTransfer> from;
        private List<ResourceGroupTransfer> to;

        private Builder() {
        }

        public Builder withRequests(@NonNull ResourceGroupLimit requests) {
            this.requests = requests;
            return this;
        }

        public Builder withLimits(@NonNull ResourceGroupLimit limits) {
            this.limits = limits;
            return this;
        }

        /**
         * Set the transfer from list.
         * 
         * @param from
         * @return
         */
        public Builder withFrom(@NonNull List<ResourceGroupTransfer> from) {
            this.from = from;
            return this;
        }

        /**
         * Set the transfer to list.
         * 
         * @param to
         * @return
         */
        public Builder withTo(@NonNull List<ResourceGroupTransfer> to) {
            this.to = to;
            return this;
        }

        public ResourceGroupConfig build() {
            return new ResourceGroupConfig(this);
        }
    }

    public ResourceGroupConfig(@NonNull io.milvus.grpc.ResourceGroupConfig grpcConfig) {
        this.requests = new ResourceGroupLimit(grpcConfig.getRequests());
        this.limits = new ResourceGroupLimit(grpcConfig.getLimits());
        this.from = grpcConfig.getTransferFromList().stream()
                .map(transfer -> new ResourceGroupTransfer(transfer))
                .collect(Collectors.toList());
        this.to = grpcConfig.getTransferToList().stream()
                .map(transfer -> new ResourceGroupTransfer(transfer))
                .collect(Collectors.toList());
    }

    public @NonNull io.milvus.grpc.ResourceGroupConfig toGRPC() {
        io.milvus.grpc.ResourceGroupConfig.Builder builder = io.milvus.grpc.ResourceGroupConfig.newBuilder()
                .setRequests(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(requests.getNodeNum()))
                .setLimits(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(limits.getNodeNum()));
        for (ResourceGroupTransfer transfer : from) {
            builder.addTransferFrom(transfer.toGRPC());
        }
        for (ResourceGroupTransfer transfer : to) {
            builder.addTransferTo(transfer.toGRPC());
        }
        return builder.build();
    }
}
