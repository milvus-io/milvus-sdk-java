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

package io.milvus.v2.service.resourcegroup.request;

public class TransferNodeReq {
    private String sourceGroupName;
    private String targetGroupName;
    private Integer numOfNodes;

    private TransferNodeReq(TransferNodeReqBuilder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.numOfNodes = builder.numOfNodes;
    }

    public static TransferNodeReqBuilder builder() {
        return new TransferNodeReqBuilder();
    }

    public String getSourceGroupName() {
        return sourceGroupName;
    }

    public void setSourceGroupName(String sourceGroupName) {
        this.sourceGroupName = sourceGroupName;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public void setTargetGroupName(String targetGroupName) {
        this.targetGroupName = targetGroupName;
    }

    public Integer getNumOfNodes() {
        return numOfNodes;
    }

    public void setNumOfNodes(Integer numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    @Override
    public String toString() {
        return "TransferNodeReq{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", numOfNodes=" + numOfNodes +
                '}';
    }

    public static class TransferNodeReqBuilder {
        private String sourceGroupName;
        private String targetGroupName;
        private Integer numOfNodes;

        public TransferNodeReqBuilder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        public TransferNodeReqBuilder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        public TransferNodeReqBuilder numOfNodes(Integer numOfNodes) {
            this.numOfNodes = numOfNodes;
            return this;
        }

        public TransferNodeReq build() {
            return new TransferNodeReq(this);
        }
    }
}
