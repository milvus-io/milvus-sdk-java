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

package io.milvus.v2.service.utility.request;

public class CreateAliasReq {
    private String databaseName;
    private String collectionName;
    private String alias;

    private CreateAliasReq(CreateAliasReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
    }

    public static CreateAliasReqBuilder builder() {
        return new CreateAliasReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "CreateAliasReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }

    public static class CreateAliasReqBuilder {
        private String databaseName;
        private String collectionName;
        private String alias;

        public CreateAliasReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CreateAliasReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CreateAliasReqBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }

        public CreateAliasReq build() {
            return new CreateAliasReq(this);
        }
    }
}
