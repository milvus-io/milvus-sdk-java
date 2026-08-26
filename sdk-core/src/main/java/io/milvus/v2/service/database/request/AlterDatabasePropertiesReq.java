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

import java.util.HashMap;
import java.util.Map;

/**
 * Request parameters for the {@code alterDatabaseProperties} API.
 */
public class AlterDatabasePropertiesReq {
    private String databaseName;
    private Map<String, String> properties;

    private AlterDatabasePropertiesReq(AlterDatabasePropertiesReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.properties = builder.properties;
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
     * Returns the properties to alter on the database.
     *
     * @return the database properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Sets the properties to alter on the database.
     *
     * @param properties the database properties
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "AlterDatabasePropertiesReq{" +
                "databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    /**
     * Creates a new builder for {@link AlterDatabasePropertiesReq}.
     *
     * @return a new {@link AlterDatabasePropertiesReqBuilder}
     */
    public static AlterDatabasePropertiesReqBuilder builder() {
        return new AlterDatabasePropertiesReqBuilder();
    }

    public static class AlterDatabasePropertiesReqBuilder {
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private AlterDatabasePropertiesReqBuilder() {
        }

        /**
         * Sets the name of the database.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterDatabasePropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the properties to alter on the database.
         *
         * @param properties the database properties
         * @return this builder
         */
        public AlterDatabasePropertiesReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Adds a single property to alter on the database.
         *
         * @param key the property key
         * @param value the property value
         * @return this builder
         */
        public AlterDatabasePropertiesReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Builds the {@link AlterDatabasePropertiesReq}.
         *
         * @return the built request
         */
        public AlterDatabasePropertiesReq build() {
            return new AlterDatabasePropertiesReq(this);
        }
    }
}
