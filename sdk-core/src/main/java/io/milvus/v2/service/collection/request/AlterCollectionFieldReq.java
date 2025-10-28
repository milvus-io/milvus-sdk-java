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

import java.util.HashMap;
import java.util.Map;

public class AlterCollectionFieldReq {
    private String collectionName;
    private String fieldName;
    private String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionFieldReq(AlterCollectionFieldReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.databaseName = builder.databaseName;
        if (builder.properties != null) {
            this.properties.putAll(builder.properties);
        }
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "AlterCollectionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static AlterCollectionFieldReqBuilder builder() {
        return new AlterCollectionFieldReqBuilder();
    }

    public static class AlterCollectionFieldReqBuilder {
        private String collectionName;
        private String fieldName;
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private AlterCollectionFieldReqBuilder() {
        }

        public AlterCollectionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AlterCollectionFieldReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public AlterCollectionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AlterCollectionFieldReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        public AlterCollectionFieldReq build() {
            return new AlterCollectionFieldReq(this);
        }
    }
}
