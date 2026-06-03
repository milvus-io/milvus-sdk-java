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

import io.milvus.exception.ParamException;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddCollectionStructFieldReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private String description;
    private Integer maxCapacity;
    private Boolean nullable;
    private List<FieldSchema> structFields;
    private Map<String, String> typeParams;

    private AddCollectionStructFieldReq(AddCollectionStructFieldReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
        this.description = builder.description;
        this.maxCapacity = builder.maxCapacity;
        this.nullable = builder.nullable;
        this.structFields = builder.structFields;
        this.typeParams = builder.typeParams;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(Integer maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public List<FieldSchema> getStructFields() {
        return structFields;
    }

    public void setStructFields(List<FieldSchema> structFields) {
        this.structFields = structFields;
    }

    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    public void setTypeParams(Map<String, String> typeParams) {
        this.typeParams = typeParams;
    }

    public CreateCollectionReq.StructFieldSchema toStructFieldSchema() {
        if (Boolean.FALSE.equals(nullable)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Adding struct field to existing collection requires nullable=true");
        }

        AddFieldReq addFieldReq = AddFieldReq.builder()
                .fieldName(fieldName)
                .description(description)
                .maxCapacity(maxCapacity)
                .structFields(structFields)
                .build();
        try {
            CreateCollectionReq.StructFieldSchema structFieldSchema = SchemaUtils.convertFieldReqToStructFieldSchema(addFieldReq);
            structFieldSchema.setNullable(Boolean.TRUE);
            structFieldSchema.setTypeParams(typeParams);
            return structFieldSchema;
        } catch (ParamException e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "AddCollectionStructFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", description='" + description + '\'' +
                ", maxCapacity=" + maxCapacity +
                ", nullable=" + nullable +
                ", structFields=" + structFields +
                ", typeParams=" + typeParams +
                '}';
    }

    public static AddCollectionStructFieldReqBuilder builder() {
        return new AddCollectionStructFieldReqBuilder();
    }

    public static class AddCollectionStructFieldReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private String fieldName = "";
        private String description = "";
        private Integer maxCapacity;
        private Boolean nullable = Boolean.TRUE;
        private List<FieldSchema> structFields = new ArrayList<>();
        private Map<String, String> typeParams = new HashMap<>();

        private AddCollectionStructFieldReqBuilder() {
        }

        public AddCollectionStructFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AddCollectionStructFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AddCollectionStructFieldReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public AddCollectionStructFieldReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        public AddCollectionStructFieldReqBuilder maxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        public AddCollectionStructFieldReqBuilder nullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public AddCollectionStructFieldReqBuilder structFields(List<FieldSchema> structFields) {
            this.structFields = structFields;
            return this;
        }

        public AddCollectionStructFieldReqBuilder addStructField(AddFieldReq addFieldReq) {
            if (this.structFields == null) {
                this.structFields = new ArrayList<>();
            }
            this.structFields.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        public AddCollectionStructFieldReqBuilder typeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
            return this;
        }

        public AddCollectionStructFieldReqBuilder typeParam(String key, String value) {
            if (this.typeParams == null) {
                this.typeParams = new HashMap<>();
            }
            this.typeParams.put(key, value);
            return this;
        }

        public AddCollectionStructFieldReq build() {
            return new AddCollectionStructFieldReq(this);
        }
    }
}
