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

import io.milvus.common.clientenum.FunctionType;
import io.milvus.exception.ParamException;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.SchemaUtils;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class CreateCollectionReq {
    private String databaseName;
    @NonNull
    private String collectionName;
    @Builder.Default
    private String description = "";
    private Integer dimension;

    @Builder.Default
    private String primaryFieldName = "id";
    @Builder.Default
    private DataType idType = DataType.Int64;
    @Builder.Default
    private Integer maxLength = 65535;
    @Builder.Default
    private String vectorFieldName = "vector";
    @Builder.Default
    private String metricType = IndexParam.MetricType.COSINE.name();
    @Builder.Default
    private Boolean autoID = Boolean.FALSE;

    // used by quickly create collections and create collections with schema
    // Note: This property is only for fast creating collection. If user use CollectionSchema to create a collection,
    //       the CollectionSchema.enableDynamicField must equal to CreateCollectionReq.enableDynamicField.
    @Builder.Default
    private Boolean enableDynamicField = Boolean.TRUE;
    @Builder.Default
    private Integer numShards = 1;

    // create collections with schema
    private CollectionSchema collectionSchema;

    @Builder.Default
    private List<IndexParam> indexParams = new ArrayList<>();

    //private String partitionKeyField;
    private Integer numPartitions;

    @Builder.Default
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;

    @Builder.Default
    private final Map<String, String> properties = new HashMap<>();

    public static abstract class CreateCollectionReqBuilder<C extends CreateCollectionReq, B extends CreateCollectionReq.CreateCollectionReqBuilder<C, B>> {
        public B indexParam(IndexParam indexParam) {
            if(null == this.indexParams$value ){
                this.indexParams$value = new ArrayList<>();
            }
            try {
                this.indexParams$value.add(indexParam);
            }catch (UnsupportedOperationException _e){
                this.indexParams$value = new ArrayList<>(this.indexParams$value);
                this.indexParams$value.add(indexParam);
            }
            this.indexParams$set = true;
            return self();
        }

        public B enableDynamicField(Boolean enableDynamicField) {
            if (this.collectionSchema != null && (this.collectionSchema.isEnableDynamicField() != enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by CollectionSchema, not allow to set different value by enableDynamicField().");
            }
            this.enableDynamicField$value = enableDynamicField;
            this.enableDynamicField$set = true;
            return self();
        }

        public B collectionSchema(CollectionSchema collectionSchema) {
            if (this.enableDynamicField$set && (collectionSchema.isEnableDynamicField() != this.enableDynamicField$value)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by enableDynamicField(), not allow to set different value by collectionSchema.");
            }
            this.collectionSchema = collectionSchema;
            this.enableDynamicField$value = collectionSchema.isEnableDynamicField();
            this.enableDynamicField$set = true;
            return self();
        }

        public B property(String key, String value) {
            if(null == this.properties$value ){
                this.properties$value = new HashMap<>();
            }
            this.properties$value.put(key, value);
            this.properties$set = true;
            return self();
        }
    }

    @Data
    @SuperBuilder
    public static class CollectionSchema {
        @Builder.Default
        private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        @Builder.Default
        private List<CreateCollectionReq.StructFieldSchema> structFields = new ArrayList<>();
        @Builder.Default
        private boolean enableDynamicField = false;
        @Builder.Default
        private List<CreateCollectionReq.Function> functionList = new ArrayList<>();

        public CollectionSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array && addFieldReq.getElementType() == DataType.Struct) {
                structFields.add(SchemaUtils.convertFieldReqToStructFieldSchema(addFieldReq));
            } else {
                fieldSchemaList.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            }
            return this;
        }

        public CollectionSchema addFunction(Function function) {
            functionList.add(function);
            return this;
        }

        public CreateCollectionReq.FieldSchema getField(String fieldName) {
            for (CreateCollectionReq.FieldSchema field : fieldSchemaList) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
        }
    }

    @Data
    @SuperBuilder
    public static class FieldSchema {
        private String name;
        @Builder.Default
        private String description = "";
        private DataType dataType;
        @Builder.Default
        private Integer maxLength = 65535;
        private Integer dimension;
        @Builder.Default
        private Boolean isPrimaryKey = Boolean.FALSE;
        @Builder.Default
        private Boolean isPartitionKey = Boolean.FALSE;
        @Builder.Default
        private Boolean isClusteringKey = Boolean.FALSE;
        @Builder.Default
        private Boolean autoID = Boolean.FALSE;
        private DataType elementType;
        private Integer maxCapacity;
        @Builder.Default
        private Boolean isNullable = Boolean.FALSE; // only for scalar fields(not include Array fields)
        @Builder.Default
        private Object defaultValue = null; // only for scalar fields
        private Boolean enableAnalyzer; // for BM25 tokenizer
        private Map<String, Object> analyzerParams; // for BM25 tokenizer
        private Boolean enableMatch; // for BM25 keyword search

        // If a specific field, such as maxLength, has been specified, it will override the corresponding key's value in typeParams.
        private Map<String, String> typeParams;
        private Map<String, Object> multiAnalyzerParams; // for multi‑language analyzers
    }

    @Data
    @SuperBuilder
    public static class Function {
        @Builder.Default
        private String name = "";
        @Builder.Default
        private String description = "";
        @Builder.Default
        private FunctionType functionType = FunctionType.UNKNOWN;
        @Builder.Default
        private List<String> inputFieldNames = new ArrayList<>();
        @Builder.Default
        private List<String> outputFieldNames = new ArrayList<>();
        @Builder.Default
        private Map<String, String> params = new HashMap<>();

        public static abstract class FunctionBuilder<C extends Function, B extends Function.FunctionBuilder<C, B>> {
            public B param(String key, String value) {
                if(null == this.params$value ){
                    this.params$value = new HashMap<>();
                }
                this.params$value.put(key, value);
                this.params$set = true;
                return self();
            }
        }
    }

    @Data
    @SuperBuilder
    public static class StructFieldSchema {
        private String name;
        @Builder.Default
        private String description = "";
        @Builder.Default
        private List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        private Integer maxCapacity;

        public StructFieldSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array || addFieldReq.getElementType() == DataType.Struct) {
                throw new ParamException("Struct field schema does not support Array, ArrayOfVector or Struct");
            }
            fields.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        public DataType getDataType() {
            return DataType.Array;
        }

        public DataType getElementType() {
            return DataType.Struct;
        }
    }
}
