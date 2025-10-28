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

package io.milvus.v2.service.index.request;

import io.milvus.v2.common.IndexParam;

import java.util.List;

public class CreateIndexReq {
    private String databaseName;
    private String collectionName;
    private List<IndexParam> indexParams;
    private Boolean sync = Boolean.TRUE; // wait the index to complete
    private Long timeout = 60000L; // timeout value for waiting the index to complete

    private CreateIndexReq(CreateIndexReqBuilder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.indexParams = builder.indexParams;
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
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    public List<IndexParam> getIndexParams() {
        return indexParams;
    }

    public void setIndexParams(List<IndexParam> indexParams) {
        this.indexParams = indexParams;
    }

    public Boolean getSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "CreateIndexReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", indexParams=" + indexParams +
                ", sync=" + sync +
                ", timeout=" + timeout +
                '}';
    }

    public static CreateIndexReqBuilder builder() {
        return new CreateIndexReqBuilder();
    }

    public static class CreateIndexReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<IndexParam> indexParams;
        private Boolean sync = Boolean.TRUE; // wait the index to complete
        private Long timeout = 60000L; // timeout value for waiting the index to complete

        private CreateIndexReqBuilder() {
        }

        public CreateIndexReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CreateIndexReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        public CreateIndexReqBuilder indexParams(List<IndexParam> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        public CreateIndexReqBuilder sync(Boolean sync) {
            this.sync = sync;
            return this;
        }

        public CreateIndexReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public CreateIndexReq build() {
            return new CreateIndexReq(this);
        }
    }
}
