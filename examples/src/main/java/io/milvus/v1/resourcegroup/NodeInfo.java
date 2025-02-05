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

package io.milvus.v1.resourcegroup;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class NodeInfo {
    private long nodeId;
    private String address;
    private String hostname;

    private NodeInfo(Builder builder) {
        this.nodeId = builder.nodeId;
        this.address = builder.address;
        this.hostname = builder.hostname;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long nodeId;
        private String address;
        private String hostname;

        public Builder withNodeId(long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder withAddress(@NonNull String address) {
            this.address = address;
            return this;
        }

        public Builder withHostname(@NonNull String hostname) {
            this.hostname = hostname;
            return this;
        }

        public NodeInfo build() {
            return new NodeInfo(this);
        }
    }
}
