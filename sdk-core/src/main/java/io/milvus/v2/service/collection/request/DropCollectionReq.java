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

package io.milvus.v2.service.collection.request;

public class DropCollectionReq {
    private String databaseName;
    private String collectionName;
    @Deprecated
    private Boolean async;
    private Long timeout;

    private DropCollectionReq(DropCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async != null ? builder.async : Boolean.TRUE;
        this.timeout = builder.timeout != null ? builder.timeout : 60000L;
    }

    public static DropCollectionReqBuilder builder() {
        return new DropCollectionReqBuilder();
    }

    // Getters
    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    public Long getTimeout() {
        return timeout;
    }

    // Setters
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "DropCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    public static class DropCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async;
        private Long timeout;

        private DropCollectionReqBuilder() {
        }

        public DropCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DropCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        @Deprecated
        public DropCollectionReqBuilder async(Boolean async) {
            this.async = async;
            return this;
        }

        public DropCollectionReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public DropCollectionReq build() {
            return new DropCollectionReq(this);
        }
    }
}
