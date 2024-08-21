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

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class CreateCollectionReq {
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

    public static abstract class CreateCollectionReqBuilder<C extends CreateCollectionReq, B extends CreateCollectionReq.CreateCollectionReqBuilder<C, B>> {
        public B indexParam(IndexParam indexParam) {
            try {
                this.indexParams$value.add(indexParam);
            }catch (UnsupportedOperationException _e){
                this.indexParams$value = new ArrayList<>(this.indexParams$value);
                this.indexParams$value.add(indexParam);
            }
            this.indexParams$set = true;
            return self();
        }
    }

    @Data
    @SuperBuilder
    public static class CollectionSchema {
        @Builder.Default
        private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();

        public CollectionSchema addField(AddFieldReq addFieldReq) {
            CreateCollectionReq.FieldSchema fieldSchema = FieldSchema.builder()
                    .name(addFieldReq.getFieldName())
                    .dataType(addFieldReq.getDataType())
                    .description(addFieldReq.getDescription())
                    .isPrimaryKey(addFieldReq.getIsPrimaryKey())
                    .isPartitionKey(addFieldReq.getIsPartitionKey())
                    .autoID(addFieldReq.getAutoID())
                    .build();
            if (addFieldReq.getDataType().equals(DataType.Array)) {
                if (addFieldReq.getElementType() == null) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Element type, maxCapacity are required for array field");
                }
                fieldSchema.setElementType(addFieldReq.getElementType());
                fieldSchema.setMaxCapacity(addFieldReq.getMaxCapacity());
            } else if (addFieldReq.getDataType().equals(DataType.VarChar)) {
                fieldSchema.setMaxLength(addFieldReq.getMaxLength());
            } else if (addFieldReq.getDataType().equals(DataType.FloatVector) || addFieldReq.getDataType().equals(DataType.BinaryVector) ||
                    addFieldReq.getDataType().equals(DataType.Float16Vector) || addFieldReq.getDataType().equals(DataType.BFloat16Vector)) {
                if (addFieldReq.getDimension() == null) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Dimension is required for vector field");
                }
                fieldSchema.setDimension(addFieldReq.getDimension());
            }
            fieldSchemaList.add(fieldSchema);
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
        private Boolean autoID = Boolean.FALSE;
        private DataType elementType;
        private Integer maxCapacity;
    }
}
