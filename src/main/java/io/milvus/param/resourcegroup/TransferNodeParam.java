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


package io.milvus.param.resourcegroup;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class TransferNodeParam {
    private final String sourceGroupName;
    private final String targetGroupName;
    private final int nodeNumber;

    private TransferNodeParam(@NonNull Builder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.nodeNumber = builder.nodeNumber;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link TransferNodeParam} class.
     */
    public static final class Builder {
        private String sourceGroupName;
        private String targetGroupName;
        private Integer nodeNumber;

        private Builder() {
        }

        /**
         * Sets the source group name. group name cannot be empty or null.
         *
         * @param groupName source group name
         * @return <code>Builder</code>
         */
        public Builder withSourceGroupName(@NonNull String groupName) {
            this.sourceGroupName = groupName;
            return this;
        }

        /**
         * Sets the target group name. group name cannot be empty or null.
         *
         * @param groupName target group name
         * @return <code>Builder</code>
         */
        public Builder withTargetGroupName(@NonNull String groupName) {
            this.targetGroupName = groupName;
            return this;
        }

        /**
         * Specify query nodes to transfer to another resource group
         *
         * @param nodeNumber number of query nodes
         * @return <code>Builder</code>
         */
        public Builder withNodeNumber(@NonNull Integer nodeNumber) {
            this.nodeNumber = nodeNumber;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link TransferNodeParam} instance.
         *
         * @return {@link TransferNodeParam}
         */
        public TransferNodeParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(sourceGroupName, "Source group name");
            ParamUtils.CheckNullEmptyString(targetGroupName, "Target group name");

            return new TransferNodeParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link TransferNodeParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "TransferNodeParam{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                "targetGroupName='" + targetGroupName + '\'' +
                "nodeNumber='" + nodeNumber +
                '}';
    }
}
