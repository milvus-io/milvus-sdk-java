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
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * Parameters for <code>insert</code> interface.
 */
@Getter
@ToString
public class InsertParam {
    protected final List<Field> fields;
    protected final List<JsonObject> rows;

    protected final String databaseName;
    protected final String collectionName;
    protected final String partitionName;
    protected final int rowCount;

    protected InsertParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.fields = builder.fields;
        this.rowCount = builder.rowCount;
        this.rows = builder.rows;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link InsertParam} class.
     */
    public static class Builder {
        protected String databaseName;
        protected String collectionName;
        protected String partitionName = "";
        protected List<InsertParam.Field> fields;
        protected List<JsonObject> rows;
        protected int rowCount;

        protected Builder() {
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
         * Set partition name (Optional).
         * This partition name will be ignored if the collection has a partition key field.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the column data to insert. The field list cannot be empty.
         *
         * @param fields insert column data
         * @return <code>Builder</code>
         * @see InsertParam.Field
         */
        public Builder withFields(@NonNull List<InsertParam.Field> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Sets the row data to insert. The rows list cannot be empty.
         *
         * Internal class for insert data.
         * If dataType is Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar, use JsonObject.addProperty(key, value) to input;
         * If dataType is FloatVector, use JsonObject.add(key, gson.toJsonTree(List[Float]) to input;
         * If dataType is BinaryVector, use JsonObject.add(key, gson.toJsonTree(byte[])) to input;
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
        public Builder withRows(@NonNull List<JsonObject> rows) {
            this.rows = rows;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link InsertParam} instance.
         *
         * @return {@link InsertParam}
         */
        public InsertParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (CollectionUtils.isEmpty(fields) && CollectionUtils.isEmpty(rows)) {
                throw new ParamException("Fields and Rows are empty, use withFields() or withRows() to input data.");
            }
            if (CollectionUtils.isNotEmpty(fields) && CollectionUtils.isNotEmpty(rows)) {
                throw new ParamException("Only one of Fields or Rows is allowed to be non-empty.");
            }

            int count;
            if (CollectionUtils.isNotEmpty(fields)) {
                if (fields.get(0) == null) {
                    throw new ParamException("Field cannot be null." +
                            " If the field is auto-id, just ignore it from withFields()");
                }
                count = fields.get(0).getValues().size();
                checkFields(count);
            } else {
                count = rows.size();
                checkRows();
            }

            this.rowCount = count;

            if (count == 0) {
                throw new ParamException("Zero row count is not allowed");
            }

            // this method doesn't check data type, the insert() api will do this work
            return new InsertParam(this);
        }

        protected void checkFields(int count) {
            for (InsertParam.Field field : fields) {
                if (field == null) {
                    throw new ParamException("Field cannot be null." +
                            " If the field is auto-id, just ignore it from withFields()");
                }

                ParamUtils.CheckNullEmptyString(field.getName(), "Field name");

                if (field.getValues() == null || field.getValues().isEmpty()) {
                    throw new ParamException("Field value cannot be empty." +
                            " If the field is auto-id, just ignore it from withFields()");
                }
            }

            // check row count
            for (InsertParam.Field field : fields) {
                if (field.getValues().size() != count) {
                    throw new ParamException("Row count of fields must be equal");
                }
            }
        }

        protected void checkRows() {
            for (JsonObject row : rows) {
                if (row == null) {
                    throw new ParamException("Row cannot be null." +
                            " If the field is auto-id, just ignore it from withRows()");
                }

                for (String rowFieldName : row.keySet()) {
                    ParamUtils.CheckNullEmptyString(rowFieldName, "Field name");

                    if (row.get(rowFieldName) == null) {
                        throw new ParamException("Field value cannot be empty." +
                                " If the field is auto-id, just ignore it from withRows()");
                    }
                }
            }
        }
    }

    /**
     * Internal class for insert data.
     * If dataType is Bool, values is List of Boolean;
     * If dataType is Int64, values is List of Long;
     * If dataType is Float, values is List of Float;
     * If dataType is Double, values is List of Double;
     * If dataType is Varchar, values is List of String;
     * If dataType is FloatVector, values is List of List Float;
     * If dataType is BinaryVector, values is List of ByteBuffer;
     * If dataType is Array, values can be List of List Boolean/Integer/Short/Long/Float/Double/String;
     * If dataType is JSON, values is List of gson.JsonObject;
     *
     * Note:
     * If dataType is Int8/Int16/Int32, values is List of Integer or Short
     * (why? because the rpc proto only support int32/int64 type, actually Int8/Int16/Int32 use int32 type to encode/decode)
     *
     */
    @lombok.Builder
    @ToString
    public static class Field {
        private final String name;
        private final List<?> values;

        public Field(String name, List<?> values) {
            this.name = name;
            this.values = values;
        }

        /**
         * Return name of the field.
         *
         * @return <code>String</code>
         */
        public String getName() {
            return name;
        }

        /**
         * Return data of the field, in column-base.
         *
         * @return <code>List</code>
         */
        public List<?> getValues() {
            return values;
        }
    }
}
