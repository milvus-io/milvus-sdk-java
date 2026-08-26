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

package io.milvus.v2.service.snapshot.request;

/**
 * Request parameters for the {@code listRestoreSnapshotJobs} API.
 */
public class ListRestoreSnapshotJobsReq {
    private String databaseName;
    private String collectionName;

    private ListRestoreSnapshotJobsReq(ListRestoreSnapshotJobsReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
    }

    public static ListRestoreSnapshotJobsReqBuilder builder() {
        return new ListRestoreSnapshotJobsReqBuilder();
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        return "ListRestoreSnapshotJobsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    public static class ListRestoreSnapshotJobsReqBuilder {
        private String databaseName = "";
        private String collectionName = "";

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public ListRestoreSnapshotJobsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public ListRestoreSnapshotJobsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Builds a {@link ListRestoreSnapshotJobsReq}.
         *
         * @return the built request
         */
        public ListRestoreSnapshotJobsReq build() {
            return new ListRestoreSnapshotJobsReq(this);
        }
    }
}
