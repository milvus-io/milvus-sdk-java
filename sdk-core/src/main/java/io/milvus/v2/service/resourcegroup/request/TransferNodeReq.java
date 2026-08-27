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

/**
 * Request parameters for the {@code transferNode} API.
 */
public class TransferNodeReq {
    private String sourceGroupName;
    private String targetGroupName;
    private Integer numOfNodes;

    private TransferNodeReq(TransferNodeReqBuilder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.numOfNodes = builder.numOfNodes;
    }

    /**
     * Creates a new builder for {@code TransferNodeReq}.
     *
     * @return the builder
     */
    public static TransferNodeReqBuilder builder() {
        return new TransferNodeReqBuilder();
    }

    /**
     * Returns the name of the source resource group.
     *
     * @return the source resource group name
     */
    public String getSourceGroupName() {
        return sourceGroupName;
    }

    /**
     * Sets the name of the source resource group.
     *
     * @param sourceGroupName the source resource group name
     */
    public void setSourceGroupName(String sourceGroupName) {
        this.sourceGroupName = sourceGroupName;
    }

    /**
     * Returns the name of the target resource group.
     *
     * @return the target resource group name
     */
    public String getTargetGroupName() {
        return targetGroupName;
    }

    /**
     * Sets the name of the target resource group.
     *
     * @param targetGroupName the target resource group name
     */
    public void setTargetGroupName(String targetGroupName) {
        this.targetGroupName = targetGroupName;
    }

    /**
     * Returns the number of query nodes to be transferred.
     *
     * @return the number of nodes
     */
    public Integer getNumOfNodes() {
        return numOfNodes;
    }

    /**
     * Sets the number of query nodes to be transferred.
     *
     * @param numOfNodes the number of nodes
     */
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

        /**
         * Sets the name of the source resource group.
         *
         * @param sourceGroupName the source resource group name
         * @return this builder
         */
        public TransferNodeReqBuilder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        /**
         * Sets the name of the target resource group.
         *
         * @param targetGroupName the target resource group name
         * @return this builder
         */
        public TransferNodeReqBuilder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        /**
         * Sets the number of query nodes to be transferred.
         *
         * @param numOfNodes the number of nodes
         * @return this builder
         */
        public TransferNodeReqBuilder numOfNodes(Integer numOfNodes) {
            this.numOfNodes = numOfNodes;
            return this;
        }

        /**
         * Builds the {@code TransferNodeReq}.
         *
         * @return the built request
         */
        public TransferNodeReq build() {
            return new TransferNodeReq(this);
        }
    }
}
