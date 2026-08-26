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

package io.milvus.v2.service.database.request;

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code dropDatabaseProperties} API.
 */
public class DropDatabasePropertiesReq {
    private String databaseName;
    private List<String> propertyKeys;

    private DropDatabasePropertiesReq(DropDatabasePropertiesReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.propertyKeys = builder.propertyKeys;
    }

    /**
     * Returns the name of the database.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the database.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the keys of the properties to drop from the database.
     *
     * @return the property keys to drop
     */
    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Sets the keys of the properties to drop from the database.
     *
     * @param propertyKeys the property keys to drop
     */
    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropDatabasePropertiesReq{" +
                "databaseName='" + databaseName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    /**
     * Creates a new builder for {@link DropDatabasePropertiesReq}.
     *
     * @return a new {@link DropDatabasePropertiesReqBuilder}
     */
    public static DropDatabasePropertiesReqBuilder builder() {
        return new DropDatabasePropertiesReqBuilder();
    }

    public static class DropDatabasePropertiesReqBuilder {
        private String databaseName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropDatabasePropertiesReqBuilder() {
        }

        /**
         * Sets the name of the database.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropDatabasePropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the keys of the properties to drop from the database.
         *
         * @param propertyKeys the property keys to drop
         * @return this builder
         */
        public DropDatabasePropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        /**
         * Builds the {@link DropDatabasePropertiesReq}.
         *
         * @return the built request
         */
        public DropDatabasePropertiesReq build() {
            return new DropDatabasePropertiesReq(this);
        }
    }
}
