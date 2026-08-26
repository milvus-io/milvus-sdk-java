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

import io.milvus.common.resourcegroup.ResourceGroupConfig;

/**
 * Request parameters for the {@code createResourceGroup} API.
 */
public class CreateResourceGroupReq {
    private String groupName;
    private ResourceGroupConfig config;

    private CreateResourceGroupReq(CreateResourceGroupReqBuilder builder) {
        this.groupName = builder.groupName;
        this.config = builder.config;
    }

    /**
     * Creates a new builder for {@code CreateResourceGroupReq}.
     *
     * @return the builder
     */
    public static CreateResourceGroupReqBuilder builder() {
        return new CreateResourceGroupReqBuilder();
    }

    /**
     * Returns the name of the resource group to be created.
     *
     * @return the resource group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the name of the resource group to be created.
     *
     * @param groupName the resource group name
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Returns the configuration of the resource group.
     *
     * @return the resource group config
     */
    public ResourceGroupConfig getConfig() {
        return config;
    }

    /**
     * Sets the configuration of the resource group.
     *
     * @param config the resource group config
     */
    public void setConfig(ResourceGroupConfig config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "CreateResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                ", config=" + config +
                '}';
    }

    public static class CreateResourceGroupReqBuilder {
        private String groupName;
        private ResourceGroupConfig config;

        /**
         * Sets the name of the resource group to be created.
         *
         * @param groupName the resource group name
         * @return this builder
         */
        public CreateResourceGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        /**
         * Sets the configuration of the resource group.
         *
         * @param config the resource group config
         * @return this builder
         */
        public CreateResourceGroupReqBuilder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Builds the {@code CreateResourceGroupReq}.
         *
         * @return the built request
         */
        public CreateResourceGroupReq build() {
            return new CreateResourceGroupReq(this);
        }
    }
}
