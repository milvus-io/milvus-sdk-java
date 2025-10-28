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

public class ReleaseCollectionReq {
    private String databaseName;
    private String collectionName;
    @Deprecated
    private Boolean async = Boolean.TRUE;
    private Long timeout = 60000L;

    private ReleaseCollectionReq(ReleaseCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async;
        this.timeout = builder.timeout;
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

    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "ReleaseCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    public static ReleaseCollectionReqBuilder builder() {
        return new ReleaseCollectionReqBuilder();
    }

    public static class ReleaseCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async = Boolean.TRUE;
        private Long timeout = 60000L;

        private ReleaseCollectionReqBuilder() {
        }

        public ReleaseCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public ReleaseCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        @Deprecated
        public ReleaseCollectionReqBuilder async(Boolean async) {
            this.async = async;
            return this;
        }

        public ReleaseCollectionReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public ReleaseCollectionReq build() {
            return new ReleaseCollectionReq(this);
        }
    }
}
