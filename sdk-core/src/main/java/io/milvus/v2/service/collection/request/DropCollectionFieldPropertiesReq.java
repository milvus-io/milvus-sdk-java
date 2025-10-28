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

import java.util.ArrayList;
import java.util.List;

public class DropCollectionFieldPropertiesReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private List<String> propertyKeys = new ArrayList<>();

    private DropCollectionFieldPropertiesReq(DropCollectionFieldPropertiesReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
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

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropCollectionFieldPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    public static DropCollectionFieldPropertiesReqBuilder builder() {
        return new DropCollectionFieldPropertiesReqBuilder();
    }

    public static class DropCollectionFieldPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private String fieldName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropCollectionFieldPropertiesReqBuilder() {
        }

        public DropCollectionFieldPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DropCollectionFieldPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DropCollectionFieldPropertiesReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public DropCollectionFieldPropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        public DropCollectionFieldPropertiesReq build() {
            return new DropCollectionFieldPropertiesReq(this);
        }
    }
}
