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


import java.util.Objects;


public class CrossClusterTopology {
    private String sourceClusterId;
    private String targetClusterId;

    public io.milvus.grpc.CrossClusterTopology toGRPC() {
        return io.milvus.grpc.CrossClusterTopology.newBuilder()
                .setSourceClusterId(this.sourceClusterId)
                .setTargetClusterId(this.targetClusterId)
                .build();
    }

    public String getSourceClusterId() {
        return sourceClusterId;
    }

    public void setSourceClusterId(String sourceClusterId) {
        this.sourceClusterId = sourceClusterId;
    }

    public String getTargetClusterId() {
        return targetClusterId;
    }

    public void setTargetClusterId(String targetClusterId) {
        this.targetClusterId = targetClusterId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        CrossClusterTopology topology = (CrossClusterTopology) o;
        return Objects.equals(sourceClusterId, topology.sourceClusterId) && Objects.equals(targetClusterId, topology.targetClusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceClusterId, targetClusterId);
    }

    @Override
    public String toString() {
        return "CrossClusterTopology{" +
                "sourceClusterId='" + sourceClusterId + '\'' +
                ", targetClusterId='" + targetClusterId + '\'' +
                '}';
    }

    private CrossClusterTopology(Builder builder) {
        this.sourceClusterId = builder.sourceClusterId;
        this.targetClusterId = builder.targetClusterId;
    }

    public static class Builder {
        private String sourceClusterId;
        private String targetClusterId;

        public Builder sourceClusterId(String sourceClusterId) {
            this.sourceClusterId = sourceClusterId;
            return this;
        }

        public Builder targetClusterId(String targetClusterId) {
            this.targetClusterId = targetClusterId;
            return this;
        }

        public CrossClusterTopology build() {
            return new CrossClusterTopology(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
