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
 * Request parameters for the {@code updateReplicateConfiguration} CDC API.
 */
public class UpdateReplicateConfigurationReq {
    private ReplicateConfiguration replicateConfiguration;
    private boolean forcePromote;

    /**
     * Creates a new {@code UpdateReplicateConfigurationReq} builder.
     *
     * @return the builder
     */
    public static UpdateReplicateConfigurationReqBuilder builder() {
        return new UpdateReplicateConfigurationReqBuilder();
    }

    private UpdateReplicateConfigurationReq(UpdateReplicateConfigurationReqBuilder builder) {
        this.replicateConfiguration = builder.replicateConfiguration;
        this.forcePromote = builder.forcePromote;
    }

    /**
     * Returns the replicate configuration.
     *
     * @return the replicate configuration
     */
    public ReplicateConfiguration getReplicateConfiguration() {
        return replicateConfiguration;
    }

    /**
     * Sets the replicate configuration.
     *
     * @param replicateConfiguration the replicate configuration
     */
    public void setReplicateConfiguration(ReplicateConfiguration replicateConfiguration) {
        this.replicateConfiguration = replicateConfiguration;
    }

    /**
     * Returns whether a forced promote is requested.
     *
     * @return {@code true} if a forced promote is requested
     */
    public boolean isForcePromote() {
        return forcePromote;
    }

    /**
     * Sets whether a forced promote is requested.
     *
     * @param forcePromote {@code true} if a forced promote is requested
     */
    public void setForcePromote(boolean forcePromote) {
        this.forcePromote = forcePromote;
    }

    @Override
    public String toString() {
        return "UpdateReplicateConfigurationReq{" +
                "replicateConfiguration=" + replicateConfiguration +
                ", forcePromote=" + forcePromote +
                '}';
    }

    public static class UpdateReplicateConfigurationReqBuilder {
        private ReplicateConfiguration replicateConfiguration;
        private boolean forcePromote;

        /**
         * Sets the replicate configuration.
         *
         * @param replicateConfiguration the replicate configuration
         * @return this builder
         */
        public UpdateReplicateConfigurationReqBuilder replicateConfiguration(ReplicateConfiguration replicateConfiguration) {
            this.replicateConfiguration = replicateConfiguration;
            return this;
        }

        /**
         * Sets whether a forced promote is requested.
         *
         * @param forcePromote {@code true} if a forced promote is requested
         * @return this builder
         */
        public UpdateReplicateConfigurationReqBuilder forcePromote(boolean forcePromote) {
            this.forcePromote = forcePromote;
            return this;
        }

        /**
         * Builds the {@link UpdateReplicateConfigurationReq}.
         *
         * @return the request
         */
        public UpdateReplicateConfigurationReq build() {
            return new UpdateReplicateConfigurationReq(this);
        }
    }
}
