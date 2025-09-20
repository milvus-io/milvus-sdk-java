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

package io.milvus.param.dml;

import com.google.gson.JsonObject;
import io.milvus.exception.ParamException;

import java.util.List;


/**
 * Parameters for <code>upsert</code> interface.
 */
public class UpsertParam extends InsertParam {
    private UpsertParam(Builder builder) {
        super(builder);
    }

    public static UpsertParam.Builder newBuilder() {
        return new UpsertParam.Builder();
    }

    /**
     * Builder for {@link UpsertParam} class.
     */
    public static class Builder extends InsertParam.Builder {
        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            super.withDatabaseName(databaseName);
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            // Replace @NonNull logic with explicit null check
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            super.withCollectionName(collectionName);
            return this;
        }

        /**
         * Set partition name (Optional).
         * This partition name will be ignored if the collection has a partition key field.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(String partitionName) {
            // Replace @NonNull logic with explicit null check
            if (partitionName == null) {
                throw new IllegalArgumentException("partitionName cannot be null");
            }
            super.withPartitionName(partitionName);
            return this;
        }

        /**
         * Sets the column data to insert. The field list cannot be empty.
         *
         * @param fields insert column data
         * @return <code>Builder</code>
         * @see InsertParam.Field
         */
        public Builder withFields(List<Field> fields) {
            // Replace @NonNull logic with explicit null check
            if (fields == null) {
                throw new IllegalArgumentException("fields cannot be null");
            }
            super.withFields(fields);
            return this;
        }

        /**
         * Sets the row data to insert. The rows list cannot be empty.
         *
         * Internal class for insert data.
         * If dataType is Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar, use JsonObject.addProperty(key, value) to input;
         * If dataType is FloatVector, use JsonObject.add(key, gson.toJsonTree(List[Float]) to input;
         * If dataType is BinaryVector/Float16Vector/BFloat16Vector, use JsonObject.add(key, gson.toJsonTree(byte[])) to input;
         * If dataType is SparseFloatVector, use JsonObject.add(key, gson.toJsonTree(SortedMap[Long, Float])) to input;
         * If dataType is Array, use JsonObject.add(key, gson.toJsonTree(List of Boolean/Integer/Short/Long/Float/Double/String)) to input;
         * If dataType is JSON, use JsonObject.add(key, JsonElement) to input;
         *
         * Note:
         * 1. For scalar numeric values, value will be cut according to the type of the field.
         * For example:
         *   An Int8 field named "XX", you set the value to be 128 by JsonObject.add("XX", 128), the value 128 is cut to -128.
         *   An Int64 field named "XX", you set the value to be 3.9 by JsonObject.add("XX", 3.9), the value 3.9 is cut to 3.
         *
         * 2. String value can be parsed to numeric/boolean type if the value is valid.
         * For example:
         *   A Bool field named "XX", you set the value to be "TRUE" by JsonObject.add("XX", "TRUE"), the string "TRUE" is parsed as true.
         *   A Float field named "XX", you set the value to be "3.5" by JsonObject.add("XX", "3.5", the string "3.5" is parsed as 3.5.
         *
         *
         * @param rows insert row data
         * @return <code>Builder</code>
         * @see JsonObject
         */
        public Builder withRows(List<JsonObject> rows) {
            // Replace @NonNull logic with explicit null check
            if (rows == null) {
                throw new IllegalArgumentException("rows cannot be null");
            }
            super.withRows(rows);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link UpsertParam} instance.
         *
         * @return {@link UpsertParam}
         */
        public UpsertParam build() throws ParamException {
            super.build();
            return new UpsertParam(this);
        }
    }

    /**
     *
     * Warning: don't use lombok@ToString to annotate this class
     * because large number of vectors will waste time in toString() method.
     *
     */
    @Override
    public String toString() {
        return "UpsertParam{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", rowCount=" + rowCount +
                '}';
    }
}
