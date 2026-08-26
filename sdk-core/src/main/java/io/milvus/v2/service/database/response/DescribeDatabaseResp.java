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

package io.milvus.v2.service.database.response;

import java.util.HashMap;
import java.util.Map;

/**
 * Response returned by the {@code describeDatabase} API.
 */
public class DescribeDatabaseResp {
    private String databaseName;
    private Map<String, String> properties;

    private DescribeDatabaseResp(DescribeDatabaseRespBuilder builder) {
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
     * Returns the properties of the database.
     *
     * @return the database properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Sets the properties of the database.
     *
     * @param properties the database properties
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "DescribeDatabaseResp{" +
                "databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeDatabaseResp}.
     *
     * @return a new {@link DescribeDatabaseRespBuilder}
     */
    public static DescribeDatabaseRespBuilder builder() {
        return new DescribeDatabaseRespBuilder();
    }

    public static class DescribeDatabaseRespBuilder {
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private DescribeDatabaseRespBuilder() {
        }

        /**
         * Sets the name of the database.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DescribeDatabaseRespBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the properties of the database.
         *
         * @param properties the database properties
         * @return this builder
         */
        public DescribeDatabaseRespBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Builds the {@link DescribeDatabaseResp}.
         *
         * @return the built response
         */
        public DescribeDatabaseResp build() {
            return new DescribeDatabaseResp(this);
        }
    }
}
