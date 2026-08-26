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

package io.milvus.v2.service.vector.request;

import com.google.gson.JsonObject;

import java.util.List;

/**
 * Request parameters for the {@code upsert} API.
 */
public class UpsertReq {
    /**
     * Sets the row data to insert. The rows list cannot be empty.
     * <p>
     * Internal class for insert data.
     * If dataType is Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar/Text/Geometry/Timestamptz, use JsonObject.addProperty(key, value) to input;
     * If dataType is FloatVector, use JsonObject.add(key, gson.toJsonTree(List[Float]) to input;
     * If dataType is BinaryVector/Float16Vector/BFloat16Vector, use JsonObject.add(key, gson.toJsonTree(byte[])) to input;
     * If dataType is SparseFloatVector, use JsonObject.add(key, gson.toJsonTree(SortedMap[Long, Float])) to input;
     * If dataType is Array, use JsonObject.add(key, gson.toJsonTree(List of Boolean/Integer/Short/Long/Float/Double/String)) to input;
     * If dataType is Array and elementType is Struct, use JsonObject.add(key, JsonArray) to input, ensure the JsonArray is a list of JsonObject;
     * If dataType is JSON, use JsonObject.add(key, JsonElement) to input;
     * <p>
     * Note:
     * 1. For scalar numeric values, value will be cut according to the type of the field.
     * For example:
     * An Int8 field named "XX", you set the value to be 128 by JsonObject.add("XX", 128), the value 128 is cut to -128.
     * An Int64 field named "XX", you set the value to be 3.9 by JsonObject.add("XX", 3.9), the value 3.9 is cut to 3.
     * <p>
     * 2. String value can be parsed to numeric/boolean type if the value is valid.
     * For example:
     * A Bool field named "XX", you set the value to be "TRUE" by JsonObject.add("XX", "TRUE"), the string "TRUE" is parsed as true.
     * A Float field named "XX", you set the value to be "3.5" by JsonObject.add("XX", "3.5", the string "3.5" is parsed as 3.5.
     *
     */
    private List<JsonObject> data;
    private String databaseName;
    private String collectionName;
    private String partitionName;
    private boolean partialUpdate;
    private List<FieldPartialUpdateOp> fieldOps;

    private UpsertReq(UpsertReqBuilder builder) {
        this.data = builder.data;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.partialUpdate = builder.partialUpdate;
        this.fieldOps = builder.fieldOps;
    }

    /**
     * Creates a new {@code UpsertReq} builder.
     *
     * @return the builder
     */
    public static UpsertReqBuilder builder() {
        return new UpsertReqBuilder();
    }

    /**
     * Returns the row data to upsert.
     *
     * @return the row data
     */
    public List<JsonObject> getData() {
        return data;
    }

    /**
     * Sets the row data to upsert.
     *
     * @param data the row data
     */
    public void setData(List<JsonObject> data) {
        this.data = data;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the partition name to upsert into.
     *
     * @return the partition name
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Sets the partition name to upsert into.
     *
     * @param partitionName the partition name
     */
    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    /**
     * Returns whether the upsert performs a partial update.
     *
     * @return {@code true} if the upsert performs a partial update
     */
    public boolean isPartialUpdate() {
        if (partialUpdate || fieldOps == null) {
            return partialUpdate;
        }
        for (FieldPartialUpdateOp fieldOp : fieldOps) {
            if (fieldOp != null && fieldOp.getOpType() != null
                    && fieldOp.getOpType() != FieldPartialUpdateOp.OpType.REPLACE) {
                return true;
            }
        }
        return partialUpdate;
    }

    /**
     * Sets whether the upsert performs a partial update.
     *
     * @param partialUpdate {@code true} if the upsert performs a partial update
     */
    public void setPartialUpdate(boolean partialUpdate) {
        this.partialUpdate = partialUpdate;
    }

    /**
     * Returns the per-field update operations for a partial update.
     *
     * @return the field update operations
     */
    public List<FieldPartialUpdateOp> getFieldOps() {
        return fieldOps;
    }

    /**
     * Sets the per-field update operations for a partial update.
     *
     * @param fieldOps the field update operations
     */
    public void setFieldOps(List<FieldPartialUpdateOp> fieldOps) {
        this.fieldOps = fieldOps;
    }

    @Override
    public String toString() {
        return "UpsertReq{" +
                "data=" + data +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", partialUpdate=" + isPartialUpdate() +
                ", fieldOps=" + fieldOps +
                '}';
    }

    public static class UpsertReqBuilder {
        private List<JsonObject> data;
        private String databaseName = "";
        private String collectionName;
        private String partitionName = "";
        private boolean partialUpdate = false; // default value
        private List<FieldPartialUpdateOp> fieldOps = null;

        /**
         * Sets the row data to upsert.
         *
         * @param data the row data
         * @return this builder
         */
        public UpsertReqBuilder data(List<JsonObject> data) {
            this.data = data;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public UpsertReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public UpsertReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition name to upsert into.
         *
         * @param partitionName the partition name
         * @return this builder
         */
        public UpsertReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets whether the upsert performs a partial update.
         *
         * @param partialUpdate {@code true} if the upsert performs a partial update
         * @return this builder
         */
        public UpsertReqBuilder partialUpdate(boolean partialUpdate) {
            this.partialUpdate = partialUpdate;
            return this;
        }

        /**
         * Sets the per-field update operations for a partial update.
         *
         * @param fieldOps the field update operations
         * @return this builder
         */
        public UpsertReqBuilder fieldOps(List<FieldPartialUpdateOp> fieldOps) {
            this.fieldOps = fieldOps;
            return this;
        }

        /**
         * Builds the {@link UpsertReq}.
         *
         * @return the request
         */
        public UpsertReq build() {
            return new UpsertReq(this);
        }
    }

    /**
     * Describes a per-field update operation applied during a partial update.
     */
    public static class FieldPartialUpdateOp {
        private String fieldName;
        private OpType opType;

        private FieldPartialUpdateOp(FieldPartialUpdateOpBuilder builder) {
            this.fieldName = builder.fieldName;
            this.opType = builder.opType;
        }

        /**
         * Creates a new {@code FieldPartialUpdateOp} builder.
         *
         * @return the builder
         */
        public static FieldPartialUpdateOpBuilder builder() {
            return new FieldPartialUpdateOpBuilder();
        }

        /**
         * Returns the name of the field to update.
         *
         * @return the field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Sets the name of the field to update.
         *
         * @param fieldName the field name
         */
        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the update operation type.
         *
         * @return the operation type
         */
        public OpType getOpType() {
            return opType;
        }

        /**
         * Sets the update operation type.
         *
         * @param opType the operation type
         */
        public void setOpType(OpType opType) {
            this.opType = opType;
        }

        @Override
        public String toString() {
            return "FieldPartialUpdateOp{" +
                    "fieldName='" + fieldName + '\'' +
                    ", opType=" + opType +
                    '}';
        }

        /**
         * The type of operation applied to a field during a partial update.
         */
        public enum OpType {
            REPLACE,
            ARRAY_APPEND,
            ARRAY_REMOVE
        }

        public static class FieldPartialUpdateOpBuilder {
            private String fieldName;
            private OpType opType = OpType.REPLACE;

            /**
             * Sets the name of the field to update.
             *
             * @param fieldName the field name
             * @return this builder
             */
            public FieldPartialUpdateOpBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            /**
             * Sets the update operation type.
             *
             * @param opType the operation type
             * @return this builder
             */
            public FieldPartialUpdateOpBuilder opType(OpType opType) {
                this.opType = opType;
                return this;
            }

            /**
             * Builds the {@link FieldPartialUpdateOp}.
             *
             * @return the field update operation
             */
            public FieldPartialUpdateOp build() {
                return new FieldPartialUpdateOp(this);
            }
        }
    }
}
