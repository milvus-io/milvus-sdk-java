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

package io.milvus.param.highlevel.collection;

import com.google.common.collect.Lists;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>createCollection</code> interface.
 */
@Getter
@ToString
public class CreateSimpleCollectionParam {
    private final CreateCollectionParam createCollectionParam;
    private final CreateIndexParam createIndexParam;
    private final LoadCollectionParam loadCollectionParam;

    private CreateSimpleCollectionParam(CreateCollectionParam createCollectionParam, CreateIndexParam createIndexParam, LoadCollectionParam loadCollectionParam) {
        this.createCollectionParam = createCollectionParam;
        this.createIndexParam = createIndexParam;
        this.loadCollectionParam = loadCollectionParam;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateSimpleCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private int dimension;
        private MetricType metricType = MetricType.L2;
        private String description = Strings.EMPTY;
        private String primaryField;
        private String vectorField;
        private boolean autoId = Boolean.FALSE;
        private boolean syncLoad = Boolean.TRUE;

        private ConsistencyLevelEnum consistencyLevel = ConsistencyLevelEnum.BOUNDED;

        private DataType primaryFieldType = DataType.Int64;

        private Integer maxLength;

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }


        /**
         * Sets the collection vector dimension. Dimension value must be greater than zero and less than 32768.
         *
         * @param dimension collection vector dimension
         * @return <code>Builder</code>
         */
        public Builder withDimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        /**
         * Sets the metricType of vectorField. The distance metric used for the collection.
         *
         * @param metricType metricType of vectorField
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the collection description. The description can be empty. The default is "".
         *
         * @param description description of the collection
         * @return <code>Builder</code>
         */
        public Builder withDescription(@NonNull String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the primaryFiled name. The primaryField cannot be empty or null. The default is "id".
         *
         * @param primaryField primaryFiled name of the collection
         * @return <code>Builder</code>
         */
        public Builder withPrimaryField(@NonNull String primaryField) {
            this.primaryField = primaryField;
            return this;
        }

        /**
         * Sets the vectorField name. The vectorField cannot be empty or null. The default is "vector".
         *
         * @param vectorField vectorField name of the collection
         * @return <code>Builder</code>
         */
        public Builder withVectorField(@NonNull String vectorField) {
            this.vectorField = vectorField;
            return this;
        }

        /**
         * Sets the autoId. The vectorField cannot be null. The default is Boolean.False.
         *
         * @param autoId if open autoId
         * @return <code>Builder</code>
         */
        public Builder withAutoId(boolean autoId) {
            this.autoId = autoId;
            return this;
        }

        /**
         * Sets the SyncLoad when loadCollection
         *
         * @param syncLoad set to true to be sync mode
         * @return <code>Builder</code>
         */
        public Builder withSyncLoad(boolean syncLoad) {
            this.syncLoad = syncLoad;
            return this;
        }

        /**
         * Sets the consistency level. The default value is {@link ConsistencyLevelEnum#BOUNDED}.
         * @see ConsistencyLevelEnum
         *
         * @param consistencyLevel consistency level
         * @return <code>Builder</code>
         */
        public Builder withConsistencyLevel(@NonNull ConsistencyLevelEnum consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets the primaryFiled type. The primaryField type cannot be empty or null. The default is "DataType.Int64".
         *
         * @param primaryFieldType primaryFiled type of the collection
         * @return <code>Builder</code>
         */
        public Builder withPrimaryFieldType(@NonNull DataType primaryFieldType) {
            this.primaryFieldType = primaryFieldType;
            return this;
        }

        /**
         * Sets the primaryFiled maxLength.
         * If primaryFiled is specified as varchar, this parameter maxLength needs to be specified
         *
         * @param maxLength maxLength of the primary field
         * @return <code>Builder</code>
         */
        public Builder withMaxLength(@NonNull Integer maxLength) {
            this.maxLength =  maxLength;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateSimpleCollectionParam} instance.
         *
         * @return {@link CreateSimpleCollectionParam}
         */
        public CreateSimpleCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            if (dimension <= 0) {
                throw new ParamException("Dimension must be larger than 0");
            }

            if (primaryFieldType != DataType.Int64 && primaryFieldType != DataType.VarChar) {
                throw new ParamException("PrimaryFieldType only supports DataType.Int64 or DataType.VarChar");
            }

            Map<String, String> primaryTypeParams = new HashMap<>();
            if (primaryFieldType == DataType.VarChar) {
                if (maxLength == null) {
                    throw new ParamException("PrimaryField is of varchar type, you need to specify the size of maxLength");
                }
                if (maxLength <= 0) {
                    throw new ParamException("Varchar field max length must be larger than zero");
                }

                if (autoId) {
                    throw new ParamException("AutoID is not supported when the VarChar field is the primary key");
                }
                primaryTypeParams.put(Constant.VARCHAR_MAX_LENGTH, String.valueOf(maxLength));
            }

            String primaryFieldName = StringUtils.defaultIfEmpty(primaryField, Constant.PRIMARY_FIELD_NAME_DEFAULT);
            String vectorFieldName = StringUtils.defaultString(vectorField, Constant.VECTOR_FIELD_NAME_DEFAULT);

            Map<String, String> floatTypeParams = new HashMap<>();
            floatTypeParams.put(Constant.VECTOR_DIM, String.valueOf(dimension));
            List<FieldType> fieldTypes = Lists.newArrayList(
                    FieldType.newBuilder().withName(primaryFieldName).withDataType(primaryFieldType).withPrimaryKey(Boolean.TRUE).withAutoID(autoId).withTypeParams(primaryTypeParams).build(),
                    FieldType.newBuilder().withName(vectorFieldName).withDataType(DataType.FloatVector).withTypeParams(floatTypeParams).build()
            );
            CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription(description)
                    .withFieldTypes(fieldTypes)
                    .withConsistencyLevel(consistencyLevel)
                    .withEnableDynamicField(Boolean.TRUE)
                    .build();

            CreateIndexParam createIndexParam = CreateIndexParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFieldName(vectorFieldName)
                    .withIndexName(Constant.VECTOR_INDEX_NAME_DEFAULT)
                    .withMetricType(metricType)
                    .withIndexType(IndexType.AUTOINDEX)
                    .build();

            LoadCollectionParam loadCollectionParam = LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(syncLoad)
                    .build();

            return new CreateSimpleCollectionParam(createCollectionParam, createIndexParam, loadCollectionParam);
        }
    }
}
