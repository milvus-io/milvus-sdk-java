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

package io.milvus.param.index;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>dropIndex</code> interface.
 */
@Getter
public class DropIndexParam {
    private final String collectionName;
    private final String fieldName;

    private DropIndexParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>DropIndexParam</code> class.
     */
    public static final class Builder {
        private String collectionName;
        private String fieldName;

        private Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Set target field name. Field name cannot be empty or null.
         *
         * @param fieldName field name
         * @return <code>Builder</code>
         */
        public Builder withFieldName(@NonNull String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Verify parameters and create a new <code>DropIndexParam</code> instance.
         *
         * @return <code>DropIndexParam</code>
         */
        public DropIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(fieldName, "Field name");

            return new DropIndexParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>DropIndexParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "DropIndexParam{" +
                "collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
