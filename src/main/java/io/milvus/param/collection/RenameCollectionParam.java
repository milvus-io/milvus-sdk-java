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

package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

import java.util.Objects;

/**
 * Parameters for <code>renameCollection</code> interface.
 */
@Getter
public class RenameCollectionParam {
    private final String oldCollectionName;
    private final String newCollectionName;

    public RenameCollectionParam(@NonNull Builder builder) {
        this.oldCollectionName = builder.oldCollectionName;
        this.newCollectionName = builder.newCollectionName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link RenameCollectionParam} class.
     */
    public static final class Builder {
        private String oldCollectionName;

        private String newCollectionName;

        private Builder() {
        }

        /**
         * Sets the old collection name. Old collection name cannot be empty or null.
         *
         * @param oldCollectionName old collection name
         * @return <code>Builder</code>
         */
        public Builder withOldCollectionName(@NonNull String oldCollectionName) {
            this.oldCollectionName = oldCollectionName;
            return this;
        }

        /**
         * Sets the new collection name. New collection name cannot be empty or null.
         *
         * @param newCollectionName new collection name
         * @return <code>Builder</code>
         */
        public Builder withNewCollectionName(@NonNull String newCollectionName) {
            this.newCollectionName = newCollectionName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link RenameCollectionParam} instance.
         *
         * @return {@link RenameCollectionParam}
         */
        public RenameCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(oldCollectionName, "Old collection name");
            ParamUtils.CheckNullEmptyString(newCollectionName, "New collection name");

            return new RenameCollectionParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link RenameCollectionParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "RenameCollectionParam{" +
                "oldCollectionName='" + oldCollectionName + '\'' +
                ", NewCollectionName=" + newCollectionName + '\'' +
                '}';
    }
}
