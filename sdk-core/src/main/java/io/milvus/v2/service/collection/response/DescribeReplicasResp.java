package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.ReplicaInfo;

import java.util.ArrayList;
import java.util.List;

public class DescribeReplicasResp {
    private List<ReplicaInfo> replicas;

    private DescribeReplicasResp(DescribeReplicasRespBuilder builder) {
        this.replicas = builder.replicas != null ? builder.replicas : new ArrayList<>();
    }

    public static DescribeReplicasRespBuilder builder() {
        return new DescribeReplicasRespBuilder();
    }

    // Getter
    public List<ReplicaInfo> getReplicas() {
        return replicas;
    }

    // Setter
    public void setReplicas(List<ReplicaInfo> replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "DescribeReplicasResp{" +
                "replicas=" + replicas +
                '}';
    }

    public static class DescribeReplicasRespBuilder {
        private List<ReplicaInfo> replicas;

        public DescribeReplicasRespBuilder replicas(List<ReplicaInfo> replicas) {
            this.replicas = replicas;
            return this;
        }

        public DescribeReplicasResp build() {
            return new DescribeReplicasResp(this);
        }
    }
}
