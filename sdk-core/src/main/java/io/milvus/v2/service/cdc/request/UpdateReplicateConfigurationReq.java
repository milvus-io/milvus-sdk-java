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

public class UpdateReplicateConfigurationReq {
    private ReplicateConfiguration replicateConfiguration;

    public static UpdateReplicateConfigurationReqBuilder builder() {
        return new UpdateReplicateConfigurationReqBuilder();
    }

    private UpdateReplicateConfigurationReq(UpdateReplicateConfigurationReqBuilder builder) {
        this.replicateConfiguration = builder.replicateConfiguration;
    }

    public ReplicateConfiguration getReplicateConfiguration() {
        return replicateConfiguration;
    }

    public void setReplicateConfiguration(ReplicateConfiguration replicateConfiguration) {
        this.replicateConfiguration = replicateConfiguration;
    }

    @Override
    public String toString() {
        return "UpdateReplicateConfigurationReq{" +
                "replicateConfiguration=" + replicateConfiguration +
                '}';
    }

    public static class UpdateReplicateConfigurationReqBuilder {
        private ReplicateConfiguration replicateConfiguration;

        public UpdateReplicateConfigurationReqBuilder replicateConfiguration(ReplicateConfiguration replicateConfiguration) {
            this.replicateConfiguration = replicateConfiguration;
            return this;
        }

        public UpdateReplicateConfigurationReq build() {
            return new UpdateReplicateConfigurationReq(this);
        }
    }
}
