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

import static io.milvus.common.utils.RedactCredential.redactCredential;
import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;

/**
 * A Milvus cluster participating in a replication task.
 */
public class MilvusCluster {
    private String clusterId;
    private String uri;
    private String token;
    private List<String> pchannels;

    /**
     * Converts a gRPC {@code MilvusCluster} to this class.
     *
     * @param cluster the gRPC cluster
     * @return the converted cluster
     */
    public static MilvusCluster fromGRPC(io.milvus.grpc.MilvusCluster cluster) {
        io.milvus.grpc.ConnectionParam connectionParam = cluster.getConnectionParam();
        return MilvusCluster.builder()
                .clusterId(cluster.getClusterId())
                .uri(connectionParam.getUri())
                .token(connectionParam.getToken())
                .pchannels(new ArrayList<>(cluster.getPchannelsList()))
                .build();
    }

    /**
     * Converts this class to a gRPC {@code MilvusCluster}.
     *
     * @return the gRPC cluster
     */
    public io.milvus.grpc.MilvusCluster toGRPC() {
        io.milvus.grpc.ConnectionParam.Builder connectionParamBuilder = io.milvus.grpc.ConnectionParam.newBuilder()
                .setUri(this.uri);
        if (this.token != null) {
            connectionParamBuilder.setToken(this.token);
        }

        io.milvus.grpc.MilvusCluster.Builder builder = io.milvus.grpc.MilvusCluster.newBuilder()
                .setClusterId(this.clusterId)
                .setConnectionParam(connectionParamBuilder);
        if (this.pchannels != null) {
            builder.addAllPchannels(this.pchannels);
        }
        return builder.build();
    }

    private MilvusCluster(MilvusClusterBuilder builder) {
        this.clusterId = builder.clusterId;
        this.uri = builder.uri;
        this.token = builder.token;
        this.pchannels = builder.pchannels;
    }

    /**
     * Returns the ID of the cluster.
     *
     * @return the cluster ID
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the ID of the cluster.
     *
     * @param clusterId the cluster ID
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Returns the connection URI of the cluster.
     *
     * @return the connection URI
     */
    public String getUri() {
        return uri;
    }

    /**
     * Sets the connection URI of the cluster.
     *
     * @param uri the connection URI
     */
    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * Returns the access token used to authenticate against the cluster.
     *
     * @return the access token
     */
    public String getToken() {
        return token;
    }

    /**
     * Sets the access token used to authenticate against the cluster.
     *
     * @param token the access token
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * Returns the physical channels that are replicated from the cluster.
     *
     * @return the physical channels
     */
    public List<String> getPchannels() {
        return pchannels;
    }

    /**
     * Sets the physical channels that are replicated from the cluster.
     *
     * @param pchannels the physical channels
     */
    public void setPchannels(List<String> pchannels) {
        this.pchannels = pchannels;
    }

    @Override
    public String toString() {
        return "MilvusCluster{" +
                "clusterId='" + clusterId + '\'' +
                ", uri='" + redactUriUserInfo(uri) + '\'' +
                ", token='" + redactCredential(token) + '\'' +
                ", pchannels=" + pchannels +
                '}';
    }

    /**
     * Creates a new {@code MilvusCluster} builder.
     *
     * @return the builder
     */
    public static MilvusClusterBuilder builder() {
        return new MilvusClusterBuilder();
    }

    /**
     * Builder for {@link MilvusCluster}.
     */
    public static class MilvusClusterBuilder {
        private String clusterId;
        private String uri;
        private String token;
        private List<String> pchannels;

        /**
         * Sets the ID of the cluster.
         *
         * @param clusterId the cluster ID
         * @return this builder
         */
        public MilvusClusterBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the connection URI of the cluster.
         *
         * @param uri the connection URI
         * @return this builder
         */
        public MilvusClusterBuilder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the access token used to authenticate against the cluster.
         *
         * @param token the access token
         * @return this builder
         */
        public MilvusClusterBuilder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Sets the physical channels that are replicated from the cluster.
         *
         * @param pchannels the physical channels
         * @return this builder
         */
        public MilvusClusterBuilder pchannels(List<String> pchannels) {
            this.pchannels = pchannels;
            return this;
        }

        /**
         * Builds the {@link MilvusCluster}.
         *
         * @return the cluster
         */
        public MilvusCluster build() {
            return new MilvusCluster(this);
        }
    }
}
