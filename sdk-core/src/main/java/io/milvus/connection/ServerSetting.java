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

package io.milvus.connection;

import io.milvus.client.MilvusClient;
import io.milvus.exception.ParamException;
import io.milvus.param.ConnectParam;
import io.milvus.param.ParamUtils;
import io.milvus.param.ServerAddress;

/**
 * Defined address and Milvus clients for each server.
 */
public class ServerSetting {
    private final ServerAddress serverAddress;
    private final MilvusClient client;

    public ServerSetting(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.serverAddress = builder.serverAddress;
        this.client = builder.milvusClient;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public MilvusClient getClient() {
        return client;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private ServerAddress serverAddress;
        private MilvusClient milvusClient;

        private Builder() {
        }


        /**
         * Sets the server address
         *
         * @param serverAddress ServerAddress host,port/server
         * @return <code>Builder</code>
         */
        public Builder withHost(ServerAddress serverAddress) {
            if (serverAddress == null) {
                throw new IllegalArgumentException("serverAddress cannot be null");
            }
            this.serverAddress = serverAddress;
            return this;
        }

        /**
         * Sets the server client for a cluster
         *
         * @param milvusClient MilvusClient
         * @return <code>Builder</code>
         */
        public Builder withMilvusClient(MilvusClient milvusClient) {
            this.milvusClient = milvusClient;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ConnectParam} instance.
         *
         * @return {@link ConnectParam}
         */
        public ServerSetting build() throws ParamException {
            ParamUtils.CheckNullEmptyString(serverAddress.getHost(), "Host name");

            if (serverAddress.getPort() < 0 || serverAddress.getPort() > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }

            if (milvusClient == null) {
                throw new ParamException("Milvus client can not be empty");
            }

            return new ServerSetting(this);
        }
    }
}
