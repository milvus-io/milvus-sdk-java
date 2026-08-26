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
 * Request parameters for the {@code describeResourceGroup} API.
 */
public class DescribeResourceGroupReq {
    private String groupName;

    private DescribeResourceGroupReq(DescribeResourceGroupReqBuilder builder) {
        this.groupName = builder.groupName;
    }

    /**
     * Creates a new builder for {@code DescribeResourceGroupReq}.
     *
     * @return the builder
     */
    public static DescribeResourceGroupReqBuilder builder() {
        return new DescribeResourceGroupReqBuilder();
    }

    /**
     * Returns the name of the resource group to be described.
     *
     * @return the resource group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the name of the resource group to be described.
     *
     * @param groupName the resource group name
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "DescribeResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    public static class DescribeResourceGroupReqBuilder {
        private String groupName;

        /**
         * Sets the name of the resource group to be described.
         *
         * @param groupName the resource group name
         * @return this builder
         */
        public DescribeResourceGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        /**
         * Builds the {@code DescribeResourceGroupReq}.
         *
         * @return the built request
         */
        public DescribeResourceGroupReq build() {
            return new DescribeResourceGroupReq(this);
        }
    }
}
