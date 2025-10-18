package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.ReplicaInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DescribeReplicasResp {
    private List<ReplicaInfo> replicas;

    private DescribeReplicasResp(Builder builder) {
        this.replicas = builder.replicas != null ? builder.replicas : new ArrayList<>();
    }

    public static Builder builder() {
        return new Builder();
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        DescribeReplicasResp that = (DescribeReplicasResp) obj;
        
        return new EqualsBuilder()
                .append(replicas, that.replicas)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas);
    }

    @Override
    public String toString() {
        return "DescribeReplicasResp{" +
                "replicas=" + replicas +
                '}';
    }

    public static class Builder {
        private List<ReplicaInfo> replicas;

        public Builder replicas(List<ReplicaInfo> replicas) {
            this.replicas = replicas;
            return this;
        }

        public DescribeReplicasResp build() {
            return new DescribeReplicasResp(this);
        }
    }
}
