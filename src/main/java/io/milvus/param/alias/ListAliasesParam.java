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

package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class ListAliasesParam {
    private final String collectionName;
    private final String databaseName;

    private ListAliasesParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ListAliasesParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String databaseName;

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ListAliasesParam} instance.
         *
         * @return {@link ListAliasesParam}
         */
        public ListAliasesParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new ListAliasesParam(this);
        }
    }

}
