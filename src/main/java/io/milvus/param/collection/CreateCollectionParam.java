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

package io.milvus.param.collection;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>createCollection</code> interface.
 */
@Getter
@ToString
public class CreateCollectionParam {
    private final String collectionName;
    private final int shardsNum;
    private final String description;
    private final List<FieldType> fieldTypes;
    private final int partitionsNum;
    private final ConsistencyLevelEnum consistencyLevel;
    private final String databaseName;

    private final boolean enableDynamicField;

    private CreateCollectionParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.shardsNum = builder.shardsNum;
        this.description = builder.description;
        this.fieldTypes = builder.fieldTypes;
        this.partitionsNum = builder.partitionsNum;
        this.consistencyLevel = builder.consistencyLevel;
        this.databaseName = builder.databaseName;
        this.enableDynamicField = builder.enableDynamicField;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private int shardsNum = 2;
        private String description = "";
        private final List<FieldType> fieldTypes = new ArrayList<>();
        private int partitionsNum = 0;
        private ConsistencyLevelEnum consistencyLevel = ConsistencyLevelEnum.SESSION;
        private String databaseName;

        private boolean enableDynamicField;
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
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the shards number. The number must be greater than zero. The default value is 2.
         *
         * @param shardsNum shards number to distribute insert data into multiple data nodes and query nodes.
         * @return <code>Builder</code>
         */
        public Builder withShardsNum(int shardsNum) {
            this.shardsNum = shardsNum;
            return this;
        }

        /**
         * Sets the collection if enableDynamicField.
         *
         * @param enableDynamicField enableDynamicField of the collection
         * @return <code>Builder</code>
         */
        public Builder withEnableDynamicField(boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
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
         * Sets the schema of the collection. The schema cannot be empty or null.
         * @see FieldType
         *
         * @param fieldTypes a <code>List</code> of {@link FieldType}
         * @return <code>Builder</code>
         */
        public Builder withFieldTypes(@NonNull List<FieldType> fieldTypes) {
            this.fieldTypes.addAll(fieldTypes);
            return this;
        }

        /**
         * Adds a field schema.
         * @see FieldType
         *
         * @param fieldType a {@link FieldType} object
         * @return <code>Builder</code>
         */
        public Builder addFieldType(@NonNull FieldType fieldType) {
            this.fieldTypes.add(fieldType);
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
         * Sets the partitions number if there is partition key field. The number must be greater than zero.
         * The default value is 64(defined in server side). The upper limit is 4096(defined in server side).
         * Not allow to set this value if none of field is partition key.
         * Only one partition key field is allowed in a collection.
         *
         * @param partitionsNum partitions number
         * @return <code>Builder</code>
         */
        public Builder withPartitionsNum(int partitionsNum) {
            this.partitionsNum = partitionsNum;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateCollectionParam} instance.
         *
         * @return {@link CreateCollectionParam}
         */
        public CreateCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (shardsNum <= 0) {
                throw new ParamException("ShardNum must be larger than 0");
            }

            if (fieldTypes.isEmpty()) {
                throw new ParamException("Field numbers must be larger than 0");
            }

            boolean hasPartitionKey = false;
            for (FieldType fieldType : fieldTypes) {
                if (fieldType == null) {
                    throw new ParamException("Collection field cannot be null");
                }

                if (fieldType.isPartitionKey()) {
                    if (hasPartitionKey) {
                        throw new ParamException("Only one partition key field is allowed in a collection");
                    }
                    hasPartitionKey = true;
                }
            }

            if (partitionsNum > 0) {
                if (!hasPartitionKey) {
                    throw new ParamException("None of fields is partition key, not allow to define partition number");
                }
            }

            return new CreateCollectionParam(this);
        }
    }
}
