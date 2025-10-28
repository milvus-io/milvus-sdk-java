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
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>createCollection</code> interface.
 */
public class CreateCollectionParam {
    private final String collectionName;
    private final int shardsNum;
    private final String description;
    private final int partitionsNum;
    private final ConsistencyLevelEnum consistencyLevel;
    private final String databaseName;

    private final List<FieldType> fieldTypes;
    private final boolean enableDynamicField;
    private final Map<String, String> properties = new HashMap<>();

    private CreateCollectionParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.shardsNum = builder.shardsNum;
        this.description = builder.description;
        this.fieldTypes = builder.fieldTypes;
        this.partitionsNum = builder.partitionsNum;
        this.consistencyLevel = builder.consistencyLevel;
        this.databaseName = builder.databaseName;
        this.enableDynamicField = builder.enableDynamicField;
        this.properties.putAll(builder.properties);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods
    public String getCollectionName() {
        return collectionName;
    }

    public int getShardsNum() {
        return shardsNum;
    }

    public String getDescription() {
        return description;
    }

    public int getPartitionsNum() {
        return partitionsNum;
    }

    public ConsistencyLevelEnum getConsistencyLevel() {
        return consistencyLevel;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<FieldType> getFieldTypes() {
        return fieldTypes;
    }

    public boolean isEnableDynamicField() {
        return enableDynamicField;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // toString method
    @Override
    public String toString() {
        return "CreateCollectionParam{" +
                "collectionName='" + collectionName + '\'' +
                ", shardsNum=" + shardsNum +
                ", description='" + description + '\'' +
                ", partitionsNum=" + partitionsNum +
                ", consistencyLevel=" + consistencyLevel +
                ", databaseName='" + databaseName + '\'' +
                ", fieldTypes=" + fieldTypes +
                ", enableDynamicField=" + enableDynamicField +
                ", properties=" + properties +
                '}';
    }

    /**
     * Builder for {@link CreateCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private int shardsNum = 0; // default to 0, let server decide the value
        private String description = "";
        private List<FieldType> fieldTypes = new ArrayList<>();
        private int partitionsNum = 0;
        private ConsistencyLevelEnum consistencyLevel = ConsistencyLevelEnum.BOUNDED;
        private String databaseName;
        private CollectionSchemaParam schema;

        private boolean enableDynamicField;

        private final Map<String, String> properties = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
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
         * Sets the shards number. The number must be greater or equal to zero.
         * The default value is 0, which means letting the server decide the value.
         * The server set this value to 1 if user didn't specify it.
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
         * @deprecated Use {@link #withSchema(CollectionSchemaParam)} repace
         */
        @Deprecated
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
        public Builder withDescription(String description) {
            if (description == null) {
                throw new IllegalArgumentException("Description cannot be null");
            }
            this.description = description;
            return this;
        }

        /**
         * Sets the schema of the collection. The schema cannot be empty or null.
         *
         * @param fieldTypes a <code>List</code> of {@link FieldType}
         * @return <code>Builder</code>
         * @see FieldType
         * @deprecated Use {@link #withSchema(CollectionSchemaParam)} repace
         */
        @Deprecated
        public Builder withFieldTypes(List<FieldType> fieldTypes) {
            if (fieldTypes == null) {
                throw new IllegalArgumentException("FieldTypes cannot be null");
            }
            this.fieldTypes.addAll(fieldTypes);
            return this;
        }

        /**
         * Adds a field schema.
         *
         * @param fieldType a {@link FieldType} object
         * @return <code>Builder</code>
         * @see FieldType
         * @deprecated Use {@link #withSchema(CollectionSchemaParam)} repace
         */
        @Deprecated
        public Builder addFieldType(FieldType fieldType) {
            if (fieldType == null) {
                throw new IllegalArgumentException("FieldType cannot be null");
            }
            this.fieldTypes.add(fieldType);
            return this;
        }

        /**
         * Sets the consistency level. The default value is {@link ConsistencyLevelEnum#BOUNDED}.
         *
         * @param consistencyLevel consistency level
         * @return <code>Builder</code>
         * @see ConsistencyLevelEnum
         */
        public Builder withConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
            if (consistencyLevel == null) {
                throw new IllegalArgumentException("ConsistencyLevel cannot be null");
            }
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
         * Sets the schema of collection.
         *
         * @param schema the schema of collection
         * @return <code>Builder</code>
         */
        public Builder withSchema(CollectionSchemaParam schema) {
            if (schema == null) {
                throw new IllegalArgumentException("Schema cannot be null");
            }
            this.schema = schema;
            return this;
        }

        /**
         * Sets the replica number in collection level, then if load collection doesn't have replica number, it will use this replica number.
         *
         * @param replicaNumber replica number
         * @return <code>Builder</code>
         */
        public Builder withReplicaNumber(int replicaNumber) {
            return this.withProperty(Constant.COLLECTION_REPLICA_NUMBER, Integer.toString(replicaNumber));
        }

        /**
         * Sets the resource groups in collection level, then if load collection doesn't have resource groups, it will use this resource groups.
         *
         * @param resourceGroups resource group names
         * @return <code>Builder</code>
         */
        public Builder withResourceGroups(List<String> resourceGroups) {
            if (resourceGroups == null) {
                throw new IllegalArgumentException("ResourceGroups cannot be null");
            }
            return this.withProperty(Constant.COLLECTION_RESOURCE_GROUPS, String.join(",", resourceGroups));

        }

        /**
         * Basic method to set a key-value property.
         *
         * @param key   the key
         * @param value the value
         * @return <code>Builder</code>
         */
        public Builder withProperty(String key, String value) {
            if (key == null) {
                throw new IllegalArgumentException("Key cannot be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("Value cannot be null");
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateCollectionParam} instance.
         *
         * @return {@link CreateCollectionParam}
         */
        public CreateCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (shardsNum < 0) {
                throw new ParamException("ShardNum must be larger or equal to 0");
            }

            if (!fieldTypes.isEmpty() && schema != null) {
                throw new ParamException("Please use either withFieldTypes(), addFieldType(), or withSchema(), and do not use them simultaneously.");
            }

            if (schema != null) {
                fieldTypes = schema.getFieldTypes();
                enableDynamicField = schema.isEnableDynamicField();
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
