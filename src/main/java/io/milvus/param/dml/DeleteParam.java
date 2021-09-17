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

package io.milvus.param.dml;

import javax.annotation.Nonnull;

public class DeleteParam {
    private final String dbName;
    private final String collectionName;
    private final String partitionName;

    private DeleteParam(@Nonnull Builder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public static class Builder {
        private String dbName = "";
        private String collectionName;
        private String partitionName;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setPartitionName(@Nonnull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder setDbName(@Nonnull String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder setCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DeleteParam build() {
            return new DeleteParam(this);
        }
    }
}
