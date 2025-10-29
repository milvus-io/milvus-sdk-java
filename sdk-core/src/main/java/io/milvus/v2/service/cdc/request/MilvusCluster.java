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

public class MilvusCluster {
    private String clusterId;
    private String uri;
    private String token;
    private List<String> pchannels;

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

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public List<String> getPchannels() {
        return pchannels;
    }

    public void setPchannels(List<String> pchannels) {
        this.pchannels = pchannels;
    }

    @Override
    public String toString() {
        return "MilvusCluster{" +
                "clusterId='" + clusterId + '\'' +
                ", uri='" + uri + '\'' +
                ", token='" + token + '\'' +
                ", pchannels=" + pchannels +
                '}';
    }

    public static MilvusClusterBuilder builder() {
        return new MilvusClusterBuilder();
    }

    public static class MilvusClusterBuilder {
        private String clusterId;
        private String uri;
        private String token;
        private List<String> pchannels;

        public MilvusClusterBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public MilvusClusterBuilder uri(String uri) {
            this.uri = uri;
            return this;
        }

        public MilvusClusterBuilder token(String token) {
            this.token = token;
            return this;
        }

        public MilvusClusterBuilder pchannels(List<String> pchannels) {
            this.pchannels = pchannels;
            return this;
        }

        public MilvusCluster build() {
            return new MilvusCluster(this);
        }
    }
}
