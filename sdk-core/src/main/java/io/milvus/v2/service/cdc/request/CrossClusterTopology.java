/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.service.cdc.request;


/**
 * A cross-cluster topology describing the replication relation between a source cluster and a
 * target cluster.
 */
public class CrossClusterTopology {
    private String sourceClusterId;
    private String targetClusterId;

    /**
     * Converts a gRPC {@code CrossClusterTopology} to this class.
     *
     * @param topology the gRPC topology
     * @return the converted topology
     */
    public static CrossClusterTopology fromGRPC(io.milvus.grpc.CrossClusterTopology topology) {
        return CrossClusterTopology.builder()
                .sourceClusterId(topology.getSourceClusterId())
                .targetClusterId(topology.getTargetClusterId())
                .build();
    }

    /**
     * Converts this class to a gRPC {@code CrossClusterTopology}.
     *
     * @return the gRPC topology
     */
    public io.milvus.grpc.CrossClusterTopology toGRPC() {
        return io.milvus.grpc.CrossClusterTopology.newBuilder()
                .setSourceClusterId(this.sourceClusterId)
                .setTargetClusterId(this.targetClusterId)
                .build();
    }

    /**
     * Returns the ID of the source cluster.
     *
     * @return the source cluster ID
     */
    public String getSourceClusterId() {
        return sourceClusterId;
    }

    /**
     * Sets the ID of the source cluster.
     *
     * @param sourceClusterId the source cluster ID
     */
    public void setSourceClusterId(String sourceClusterId) {
        this.sourceClusterId = sourceClusterId;
    }

    /**
     * Returns the ID of the target cluster.
     *
     * @return the target cluster ID
     */
    public String getTargetClusterId() {
        return targetClusterId;
    }

    /**
     * Sets the ID of the target cluster.
     *
     * @param targetClusterId the target cluster ID
     */
    public void setTargetClusterId(String targetClusterId) {
        this.targetClusterId = targetClusterId;
    }

    @Override
    public String toString() {
        return "CrossClusterTopology{" +
                "sourceClusterId='" + sourceClusterId + '\'' +
                ", targetClusterId='" + targetClusterId + '\'' +
                '}';
    }

    private CrossClusterTopology(CrossClusterTopologyBuilder builder) {
        this.sourceClusterId = builder.sourceClusterId;
        this.targetClusterId = builder.targetClusterId;
    }

    /**
     * Creates a new {@code CrossClusterTopology} builder.
     *
     * @return the builder
     */
    public static CrossClusterTopologyBuilder builder() {
        return new CrossClusterTopologyBuilder();
    }

    /**
     * Builder for {@link CrossClusterTopology}.
     */
    public static class CrossClusterTopologyBuilder {
        private String sourceClusterId;
        private String targetClusterId;

        /**
         * Sets the ID of the source cluster.
         *
         * @param sourceClusterId the source cluster ID
         * @return this builder
         */
        public CrossClusterTopologyBuilder sourceClusterId(String sourceClusterId) {
            this.sourceClusterId = sourceClusterId;
            return this;
        }

        /**
         * Sets the ID of the target cluster.
         *
         * @param targetClusterId the target cluster ID
         * @return this builder
         */
        public CrossClusterTopologyBuilder targetClusterId(String targetClusterId) {
            this.targetClusterId = targetClusterId;
            return this;
        }

        /**
         * Builds the {@link CrossClusterTopology}.
         *
         * @return the topology
         */
        public CrossClusterTopology build() {
            return new CrossClusterTopology(this);
        }
    }
}
