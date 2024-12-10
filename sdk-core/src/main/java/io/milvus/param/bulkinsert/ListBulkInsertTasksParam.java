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

package io.milvus.param.bulkinsert;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>listBulkInsertTasks</code> interface.
 */
@Getter
@ToString
public class ListBulkInsertTasksParam {
    private final String databaseName;
    private final String collectionName;
    private final int limit;

    private ListBulkInsertTasksParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.limit = builder.limit;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ListBulkInsertTasksParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName = ""; // empty string will list all tasks in the server side

        // The limit count of returned tasks, list all tasks if the value is 0
        // default by 0
        private Integer limit = 0;

        private Builder() {
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
         * Sets the target collection name, list all tasks if the name is empty.
         * Default value is an empty string.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Specify limit count of returned tasks, list all tasks if the value is 0.
         * Default value is 0
         *
         * @param limit limit number
         * @return <code>Builder</code>
         */
        public Builder withLimit(@NonNull Integer limit) {
            this.limit = limit;
            if (this.limit < 0) {
                this.limit = 0;
            }
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ListBulkInsertTasksParam} instance.
         *
         * @return {@link ListBulkInsertTasksParam}
         */
        public ListBulkInsertTasksParam build() throws ParamException {
            return new ListBulkInsertTasksParam(this);
        }
    }

}
