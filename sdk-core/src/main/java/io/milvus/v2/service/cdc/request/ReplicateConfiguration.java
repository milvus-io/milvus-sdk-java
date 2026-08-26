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

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of a replication task, including the participating clusters and the cross-cluster
 * topologies between them.
 */
public class ReplicateConfiguration {
    private List<MilvusCluster> clusters;
    private List<CrossClusterTopology> crossClusterTopologies;

    /**
     * Converts a gRPC {@code ReplicateConfiguration} to this class.
     *
     * @param configuration the gRPC configuration
     * @return the converted configuration
     */
    public static ReplicateConfiguration fromGRPC(io.milvus.grpc.ReplicateConfiguration configuration) {
        List<MilvusCluster> clusters = new ArrayList<>();
        configuration.getClustersList().forEach(cluster -> clusters.add(MilvusCluster.fromGRPC(cluster)));

        List<CrossClusterTopology> crossClusterTopologies = new ArrayList<>();
        configuration.getCrossClusterTopologyList().forEach(topology ->
                crossClusterTopologies.add(CrossClusterTopology.fromGRPC(topology)));

        return ReplicateConfiguration.builder()
                .clusters(clusters)
                .crossClusterTopologies(crossClusterTopologies)
                .build();
    }

    /**
     * Converts this class to a gRPC {@code ReplicateConfiguration}.
     *
     * @return the gRPC configuration
     */
    public io.milvus.grpc.ReplicateConfiguration toGRPC() {
        io.milvus.grpc.ReplicateConfiguration.Builder builder = io.milvus.grpc.ReplicateConfiguration.newBuilder();
        if (this.clusters != null) {
            for (MilvusCluster cluster : this.clusters) {
                builder.addClusters(cluster.toGRPC());
            }
        }

        if (this.crossClusterTopologies != null) {
            for (CrossClusterTopology topology : this.crossClusterTopologies) {
                builder.addCrossClusterTopology(topology.toGRPC());
            }
        }

        return builder.build();
    }

    private ReplicateConfiguration(ReplicateConfigurationBuilder builder) {
        this.clusters = builder.clusters;
        this.crossClusterTopologies = builder.crossClusterTopologies;
    }

    /**
     * Returns the clusters participating in the replication task.
     *
     * @return the clusters
     */
    public List<MilvusCluster> getClusters() {
        return clusters;
    }

    /**
     * Sets the clusters participating in the replication task.
     *
     * @param clusters the clusters
     */
    public void setClusters(List<MilvusCluster> clusters) {
        this.clusters = clusters;
    }

    /**
     * Returns the cross-cluster topologies of the replication task.
     *
     * @return the cross-cluster topologies
     */
    public List<CrossClusterTopology> getCrossClusterTopologies() {
        return crossClusterTopologies;
    }

    /**
     * Sets the cross-cluster topologies of the replication task.
     *
     * @param crossClusterTopologies the cross-cluster topologies
     */
    public void setCrossClusterTopologies(List<CrossClusterTopology> crossClusterTopologies) {
        this.crossClusterTopologies = crossClusterTopologies;
    }

    @Override
    public String toString() {
        return "ReplicateConfiguration{" +
                "clusters=" + clusters +
                ", crossClusterTopologies=" + crossClusterTopologies +
                '}';
    }

    /**
     * Creates a new {@code ReplicateConfiguration} builder.
     *
     * @return the builder
     */
    public static ReplicateConfigurationBuilder builder() {
        return new ReplicateConfigurationBuilder();
    }

    /**
     * Builder for {@link ReplicateConfiguration}.
     */
    public static class ReplicateConfigurationBuilder {
        private List<MilvusCluster> clusters;
        private List<CrossClusterTopology> crossClusterTopologies;

        /**
         * Sets the clusters participating in the replication task.
         *
         * @param clusters the clusters
         * @return this builder
         */
        public ReplicateConfigurationBuilder clusters(List<MilvusCluster> clusters) {
            this.clusters = clusters;
            return this;
        }

        /**
         * Sets the cross-cluster topologies of the replication task.
         *
         * @param crossClusterTopologies the cross-cluster topologies
         * @return this builder
         */
        public ReplicateConfigurationBuilder crossClusterTopologies(List<CrossClusterTopology> crossClusterTopologies) {
            this.crossClusterTopologies = crossClusterTopologies;
            return this;
        }

        /**
         * Builds the {@link ReplicateConfiguration}.
         *
         * @return the configuration
         */
        public ReplicateConfiguration build() {
            return new ReplicateConfiguration(this);
        }
    }
}
