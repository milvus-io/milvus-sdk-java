package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.ReplicaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Response of the {@code describeReplicas} API, holding the replicas of a collection.
 */
public class DescribeReplicasResp {
    private List<ReplicaInfo> replicas;

    private DescribeReplicasResp(DescribeReplicasRespBuilder builder) {
        this.replicas = builder.replicas != null ? builder.replicas : new ArrayList<>();
    }

    /**
     * Creates a new builder for {@link DescribeReplicasResp}.
     *
     * @return the builder
     */
    public static DescribeReplicasRespBuilder builder() {
        return new DescribeReplicasRespBuilder();
    }

    // Getter
    /**
     * Returns the replicas of the collection.
     *
     * @return the replicas
     */
    public List<ReplicaInfo> getReplicas() {
        return replicas;
    }

    // Setter
    /**
     * Sets the replicas of the collection.
     *
     * @param replicas the replicas
     */
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

        /**
         * Sets the replicas of the collection.
         *
         * @param replicas the replicas
         * @return this builder
         */
        public DescribeReplicasRespBuilder replicas(List<ReplicaInfo> replicas) {
            this.replicas = replicas;
            return this;
        }

        /**
         * Builds a {@link DescribeReplicasResp} with the configured parameters.
         *
         * @return the response
         */
        public DescribeReplicasResp build() {
            return new DescribeReplicasResp(this);
        }
    }
}
