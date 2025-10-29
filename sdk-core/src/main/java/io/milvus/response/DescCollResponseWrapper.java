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

package io.milvus.response;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;

import java.util.*;

/**
 * Util class to wrap response of <code>describeCollection</code> interface.
 */
public class DescCollResponseWrapper {
    private final DescribeCollectionResponse response;

    Map<String, String> pairs = new HashMap<>();

    public DescCollResponseWrapper(DescribeCollectionResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("Response cannot be null");
        }
        this.response = response;
        response.getPropertiesList().forEach((prop) -> pairs.put(prop.getKey(), prop.getValue()));
    }

    /**
     * Get name of the collection.
     *
     * @return <code>String</code> name of the collection
     */
    public String getCollectionName() {
        CollectionSchema schema = response.getSchema();
        return schema.getName();
    }

    /**
     * Get database name of the collection.
     *
     * @return <code>String</code> name of the database
     */
    public String getDatabaseName() {
        return response.getDbName();
    }

    /**
     * Get description of the collection.
     *
     * @return <code>String</code> description of the collection
     */
    public String getCollectionDescription() {
        CollectionSchema schema = response.getSchema();
        return schema.getDescription();
    }

    /**
     * Get internal id of the collection.
     *
     * @return <code>long</code> internal id of the collection
     */
    public long getCollectionID() {
        return response.getCollectionID();
    }

    /**
     * Get shard number of the collection.
     *
     * @return <code>int</code> shard number of the collection
     */
    public int getShardNumber() {
        return response.getShardsNum();
    }

    /**
     * Get consistency level of the collection.
     *
     * @return <code>ConsistencyLevelEnum</code> consistency level of the collection
     */
    public ConsistencyLevelEnum getConsistencyLevel() {
        // may throw IllegalArgumentException
        return ConsistencyLevelEnum.valueOf(response.getConsistencyLevel().name().toUpperCase());
    }

    /**
     * Get utc timestamp when collection created.
     *
     * @return <code>long</code> utc timestamp when collection created
     */
    public long getCreatedUtcTimestamp() {
        return response.getCreatedUtcTimestamp();
    }

    /**
     * Get aliases of the collection.
     *
     * @return List of String, aliases of the collection
     */
    public List<String> getAliases() {
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < response.getAliasesCount(); ++i) {
            aliases.add(response.getAliases(i));
        }

        return aliases;
    }

    /**
     * Get schema of the collection's fields.
     *
     * @return List of FieldType, schema of the collection's fields
     */
    public List<FieldType> getFields() {
        List<FieldType> results = new ArrayList<>();
        CollectionSchema schema = response.getSchema();
        List<FieldSchema> fields = schema.getFieldsList();
        fields.forEach((field) -> results.add(ParamUtils.ConvertField(field)));

        return results;
    }

    /**
     * Get schema of a field by name.
     * Return null if the field doesn't exist
     *
     * @param fieldName field name to get field description
     * @return {@link FieldType} schema of the field
     */
    public FieldType getFieldByName(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name cannot be null");
        }
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (fieldName.compareTo(field.getName()) == 0) {
                return ParamUtils.ConvertField(field);
            }
        }

        return null;
    }

    // duplicated with isDynamicFieldEnabled()
    @Deprecated
    public boolean getEnableDynamicField() {
        CollectionSchema schema = response.getSchema();
        return schema.getEnableDynamicField();
    }

    /**
     * Get whether the collection dynamic field is enabled
     *
     * @return boolean
     */
    public boolean isDynamicFieldEnabled() {
        CollectionSchema schema = response.getSchema();
        return schema.getEnableDynamicField();
    }

    /**
     * Get the partition key field.
     * Return null if the partition key field doesn't exist.
     *
     * @return {@link FieldType} schema of the partition key field
     */
    public FieldType getPartitionKeyField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (field.getIsPartitionKey()) {
                return ParamUtils.ConvertField(field);
            }
        }

        return null;
    }

    /**
     * Get the primary key field.
     * throw ParamException if the primary key field doesn't exist.
     *
     * @return {@link FieldType} schema of the primary key field
     */
    public FieldType getPrimaryField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (field.getIsPrimaryKey()) {
                return ParamUtils.ConvertField(field);
            }
        }

        throw new ParamException("No primary key found.");
    }

    /**
     * Get the vector key field.
     * throw ParamException if the vector key field doesn't exist.
     * This method is deprecated since Milvus supports multiple vector fields from v2.4
     *
     * @return {@link FieldType} schema of the vector key field
     */
    @Deprecated
    public FieldType getVectorField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (ParamUtils.isVectorDataType(field.getDataType())) {
                return ParamUtils.ConvertField(field);
            }
        }

        throw new ParamException("No vector key found.");
    }

    /**
     * Get all the vector key field. (Milvus supports multiple vector fields from v2.4)
     *
     * @return {@link FieldType} schema of the vector key field
     */
    public List<FieldType> getVectorFields() {
        List<FieldType> vectorFields = new ArrayList<>();
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (ParamUtils.isVectorDataType(field.getDataType())) {
                vectorFields.add(ParamUtils.ConvertField(field));
            }
        }

        return vectorFields;
    }

    /**
     * Get properties of the collection.
     *
     * @return List of String, aliases of the collection
     */
    public Map<String, String> getProperties() {
        return pairs;
    }

    /**
     * Get the collection schema of collection
     *
     * @return {@link CollectionSchemaParam} schema of the collection
     */
    public CollectionSchemaParam getSchema() {
        return CollectionSchemaParam.newBuilder()
                .withFieldTypes(getFields())
                .withEnableDynamicField(isDynamicFieldEnabled())
                .build();
    }


    /**
     * return collection resource groups
     *
     * @return resource group names
     */
    public List<String> getResourceGroups() {
        String value = pairs.get(Constant.COLLECTION_RESOURCE_GROUPS);
        if (value == null) {
            return new ArrayList<>();
        }
        return Arrays.asList(value.split(","));
    }

    /**
     * return collection replica number
     *
     * @return replica number
     */
    public int getReplicaNumber() {
        String value = pairs.get(Constant.COLLECTION_REPLICA_NUMBER);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(pairs.get(Constant.COLLECTION_REPLICA_NUMBER));
    }

    /**
     * Construct a <code>String</code> by {@link DescCollResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Collection Description{" +
                "name:'" + getCollectionName() + '\'' +
                ", databaseName:'" + getDatabaseName() + '\'' +
                ", description:'" + getCollectionDescription() + '\'' +
                ", id:" + getCollectionID() +
                ", shardNumber:" + getShardNumber() +
                ", createdUtcTimestamp:" + getCreatedUtcTimestamp() +
                ", aliases:" + getAliases().toString() +
                ", fields:" + getFields().toString() +
                ", isDynamicFieldEnabled:" + isDynamicFieldEnabled() +
                ", consistencyLevel:" + getConsistencyLevel().name() +
                ", properties:" + getProperties() +
                '}';
    }
}
