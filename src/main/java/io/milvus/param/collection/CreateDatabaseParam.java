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
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>createDatabase</code> interface.
 */
@Getter
@ToString
public class CreateDatabaseParam {
    private final String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private CreateDatabaseParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.properties.putAll(builder.properties);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateDatabaseParam} class.
     */
    public static final class Builder {
        private String databaseName;

        private final Map<String, String> properties = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the database name. Database name cannot be empty or null.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(@NonNull String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

         /**
         * Sets the replica number in database level, then if load collection doesn't have replica number, it will use this replica number.
         * @param replicaNumber replica number
         * @return <code>Builder</code>
         */
        public Builder withReplicaNumber(int replicaNumber) {
            return this.withProperty(Constant.DATABASE_REPLICA_NUMBER, Integer.toString(replicaNumber));
        }

        /**
         * Sets the resource groups in database level, then if load collection doesn't have resource groups, it will use this resource groups.
         * @param resourceGroups resource group names
         * @return <code>Builder</code>
         */
        public Builder withResourceGroups(@NonNull List<String> resourceGroups) {
            return this.withProperty(Constant.DATABASE_RESOURCE_GROUPS, String.join(",", resourceGroups));

        }

        /**
         * Basic method to set a key-value property.
         *
         * @param key the key
         * @param value the value
         * @return <code>Builder</code>
         */
        public Builder withProperty(@NonNull String key, @NonNull String value) {
            this.properties.put(key, value);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateDatabaseParam} instance.
         *
         * @return {@link CreateDatabaseParam}
         */
        public CreateDatabaseParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(databaseName, "Database name");

            return new CreateDatabaseParam(this);
        }
    }

}
