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

public class GetReplicateInfoReq {
    private String sourceClusterId;
    private String targetPchannel;

    public static GetReplicateInfoReqBuilder builder() {
        return new GetReplicateInfoReqBuilder();
    }

    private GetReplicateInfoReq(GetReplicateInfoReqBuilder builder) {
        this.sourceClusterId = builder.sourceClusterId;
        this.targetPchannel = builder.targetPchannel;
    }

    public String getSourceClusterId() {
        return sourceClusterId;
    }

    public void setSourceClusterId(String sourceClusterId) {
        this.sourceClusterId = sourceClusterId;
    }

    public String getTargetPchannel() {
        return targetPchannel;
    }

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

    public static class GetReplicateInfoReqBuilder {
        private String sourceClusterId;
        private String targetPchannel;

        public GetReplicateInfoReqBuilder sourceClusterId(String sourceClusterId) {
            this.sourceClusterId = sourceClusterId;
            return this;
        }

        public GetReplicateInfoReqBuilder targetPchannel(String targetPchannel) {
            this.targetPchannel = targetPchannel;
            return this;
        }

        public GetReplicateInfoReq build() {
            return new GetReplicateInfoReq(this);
        }
    }
}
