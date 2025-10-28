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

    public static CrossClusterTopologyBuilder builder() {
        return new CrossClusterTopologyBuilder();
    }

    public static class CrossClusterTopologyBuilder {
        private String sourceClusterId;
        private String targetClusterId;

        public CrossClusterTopologyBuilder sourceClusterId(String sourceClusterId) {
            this.sourceClusterId = sourceClusterId;
            return this;
        }

        public CrossClusterTopologyBuilder targetClusterId(String targetClusterId) {
            this.targetClusterId = targetClusterId;
            return this;
        }

        public CrossClusterTopology build() {
            return new CrossClusterTopology(this);
        }
    }
}
