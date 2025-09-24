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

import java.util.List;
import java.util.Objects;

public class ReplicateConfiguration {
    private List<MilvusCluster> clusters;
    private List<CrossClusterTopology> crossClusterTopologies;

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

    private ReplicateConfiguration(Builder builder) {
        this.clusters = builder.clusters;
        this.crossClusterTopologies = builder.crossClusterTopologies;
    }

    public List<MilvusCluster> getClusters() {
        return clusters;
    }

    public void setClusters(List<MilvusCluster> clusters) {
        this.clusters = clusters;
    }

    public List<CrossClusterTopology> getCrossClusterTopologies() {
        return crossClusterTopologies;
    }

    public void setCrossClusterTopologies(List<CrossClusterTopology> crossClusterTopologies) {
        this.crossClusterTopologies = crossClusterTopologies;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplicateConfiguration that = (ReplicateConfiguration) o;
        return Objects.equals(clusters, that.clusters) && Objects.equals(crossClusterTopologies, that.crossClusterTopologies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusters, crossClusterTopologies);
    }

    @Override
    public String toString() {
        return "ReplicateConfiguration{" +
                "clusters=" + clusters +
                ", crossClusterTopologies=" + crossClusterTopologies +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<MilvusCluster> clusters;
        private List<CrossClusterTopology> crossClusterTopologies;

        public Builder clusters(List<MilvusCluster> clusters) {
            this.clusters = clusters;
            return this;
        }

        public Builder crossClusterTopologies(List<CrossClusterTopology> crossClusterTopologies) {
            this.crossClusterTopologies = crossClusterTopologies;
            return this;
        }

        public ReplicateConfiguration build() {
            return new ReplicateConfiguration(this);
        }
    }
}
