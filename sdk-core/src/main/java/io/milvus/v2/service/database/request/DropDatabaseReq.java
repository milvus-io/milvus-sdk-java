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

/**
 * Request parameters for the {@code dropDatabase} API.
 */
public class DropDatabaseReq {
    private String databaseName;

    private DropDatabaseReq(DropDatabaseReqBuilder builder) {
        this.databaseName = builder.databaseName;
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

    @Override
    public String toString() {
        return "DropDatabaseReq{" +
                "databaseName='" + databaseName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DropDatabaseReq}.
     *
     * @return a new {@link DropDatabaseReqBuilder}
     */
    public static DropDatabaseReqBuilder builder() {
        return new DropDatabaseReqBuilder();
    }

    public static class DropDatabaseReqBuilder {
        private String databaseName;

        private DropDatabaseReqBuilder() {
        }

        /**
         * Sets the name of the database.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropDatabaseReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Builds the {@link DropDatabaseReq}.
         *
         * @return the built request
         */
        public DropDatabaseReq build() {
            return new DropDatabaseReq(this);
        }
    }
}
