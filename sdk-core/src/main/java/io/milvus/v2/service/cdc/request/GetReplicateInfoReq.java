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
 * Request parameters for the {@code getReplicateInfo} CDC API.
 */
public class GetReplicateInfoReq {
    private String sourceClusterId;
    private String targetPchannel;

    /**
     * Creates a new {@code GetReplicateInfoReq} builder.
     */
    public static GetReplicateInfoReqBuilder builder() {
        return new GetReplicateInfoReqBuilder();
    }

    private GetReplicateInfoReq(GetReplicateInfoReqBuilder builder) {
        this.sourceClusterId = builder.sourceClusterId;
        this.targetPchannel = builder.targetPchannel;
    }

    /**
     * Returns the ID of the source cluster.
     */
    public String getSourceClusterId() {
        return sourceClusterId;
    }

    /**
     * Sets the ID of the source cluster.
     */
    public void setSourceClusterId(String sourceClusterId) {
        this.sourceClusterId = sourceClusterId;
    }

    /**
     * Returns the physical channel (pchannel) to query replication info for. Despite the field name,
     * the value is the pchannel belonging to the source cluster.
     */
    public String getTargetPchannel() {
        return targetPchannel;
    }

    /**
     * Sets the physical channel (pchannel) to query replication info for. Despite the field name,
     * the value is the pchannel belonging to the source cluster.
     */
    public void setTargetPchannel(String targetPchannel) {
        this.targetPchannel = targetPchannel;
    }

    @Override
    public String toString() {
        return "GetReplicateInfoReq{" +
                "sourceClusterId='" + sourceClusterId + '\'' +
                ", targetPchannel='" + targetPchannel + '\'' +
                '}';
    }

    /**
     * Builder for {@link GetReplicateInfoReq}.
     */
    public static class GetReplicateInfoReqBuilder {
        private String sourceClusterId;
        private String targetPchannel;

        /**
         * Sets the ID of the source cluster.
         */
        public GetReplicateInfoReqBuilder sourceClusterId(String sourceClusterId) {
            this.sourceClusterId = sourceClusterId;
            return this;
        }

        /**
         * Sets the physical channel (pchannel) to query replication info for. Despite the field name,
         * the value is the pchannel belonging to the source cluster.
         */
        public GetReplicateInfoReqBuilder targetPchannel(String targetPchannel) {
            this.targetPchannel = targetPchannel;
            return this;
        }

        /**
         * Builds the {@link GetReplicateInfoReq}.
         */
        public GetReplicateInfoReq build() {
            return new GetReplicateInfoReq(this);
        }
    }
}
