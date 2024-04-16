package io.milvus.resourcegroup;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class NodeInfo {
    private long nodeId;
    private String address;
    private String hostname;

    private NodeInfo(Builder builder) {
        this.nodeId = builder.nodeId;
        this.address = builder.address;
        this.hostname = builder.hostname;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long nodeId;
        private String address;
        private String hostname;

        public Builder withNodeId(long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder withAddress(@NonNull String address) {
            this.address = address;
            return this;
        }

        public Builder withHostname(@NonNull String hostname) {
            this.hostname = hostname;
            return this;
        }

        public NodeInfo build() {
            return new NodeInfo(this);
        }
    }
}
