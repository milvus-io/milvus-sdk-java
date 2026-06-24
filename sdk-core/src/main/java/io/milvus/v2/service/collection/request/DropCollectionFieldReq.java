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

public class DropCollectionFieldReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private Long fieldId;

    private DropCollectionFieldReq(DropCollectionFieldReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
        this.fieldId = builder.fieldId;
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

    public Long getFieldId() {
        return fieldId;
    }

    public void setFieldId(Long fieldId) {
        this.fieldId = fieldId;
    }

    @Override
    public String toString() {
        return "DropCollectionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", fieldId=" + fieldId +
                '}';
    }

    public static DropCollectionFieldReqBuilder builder() {
        return new DropCollectionFieldReqBuilder();
    }

    public static class DropCollectionFieldReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private String fieldName = "";
        private Long fieldId;

        private DropCollectionFieldReqBuilder() {
        }

        public DropCollectionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DropCollectionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DropCollectionFieldReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public DropCollectionFieldReqBuilder fieldId(Long fieldId) {
            this.fieldId = fieldId;
            return this;
        }

        public DropCollectionFieldReq build() {
            return new DropCollectionFieldReq(this);
        }
    }
}
