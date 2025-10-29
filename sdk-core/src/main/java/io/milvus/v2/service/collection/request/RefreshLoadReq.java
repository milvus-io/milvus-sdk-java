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

public class RefreshLoadReq {
    private String databaseName;
    private String collectionName;
    private Boolean async = Boolean.TRUE;
    private Boolean sync = Boolean.TRUE; // wait the collection to be fully loaded. "async" is deprecated, use "sync" instead
    private Long timeout = 60000L; // timeout value for waiting the collection to be fully loaded

    private RefreshLoadReq(RefreshLoadReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async;
        this.sync = builder.sync;
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

    public Boolean getAsync() {
        return async;
    }

    public void setAsync(Boolean async) {
        this.async = async;
        this.sync = !async;
    }

    public Boolean getSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
        this.async = !sync;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "RefreshLoadReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", sync=" + sync +
                ", timeout=" + timeout +
                '}';
    }

    public static RefreshLoadReqBuilder builder() {
        return new RefreshLoadReqBuilder();
    }

    public static class RefreshLoadReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async = Boolean.TRUE;
        private Boolean sync = Boolean.TRUE;
        private Long timeout = 60000L;

        private RefreshLoadReqBuilder() {
        }

        public RefreshLoadReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public RefreshLoadReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RefreshLoadReqBuilder async(Boolean async) {
            this.async = async;
            this.sync = !async;
            return this;
        }

        public RefreshLoadReqBuilder sync(Boolean sync) {
            this.sync = sync;
            this.async = !sync;
            return this;
        }

        public RefreshLoadReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public RefreshLoadReq build() {
            return new RefreshLoadReq(this);
        }
    }
}
