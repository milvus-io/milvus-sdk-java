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

import java.util.ArrayList;
import java.util.List;

public class DropIndexPropertiesReq {
    private String collectionName;
    private String databaseName;
    private String indexName;
    private List<String> propertyKeys;

    private DropIndexPropertiesReq(DropIndexPropertiesReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.indexName = builder.indexName;
        this.propertyKeys = builder.propertyKeys;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropIndexPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    public static DropIndexPropertiesReqBuilder builder() {
        return new DropIndexPropertiesReqBuilder();
    }

    public static class DropIndexPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private String indexName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropIndexPropertiesReqBuilder() {
        }

        public DropIndexPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DropIndexPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DropIndexPropertiesReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public DropIndexPropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        public DropIndexPropertiesReq build() {
            return new DropIndexPropertiesReq(this);
        }
    }
}
