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

public class InsertReq {
    //private List<> fields;

    /**
     * Sets the row data to insert. The rows list cannot be empty.
     * <p>
     * Internal class for insert data.
     * If dataType is Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar/Geometry/Timestamptz, use JsonObject.addProperty(key, value) to input;
     * If dataType is FloatVector, use JsonObject.add(key, gson.toJsonTree(List[Float]) to input;
     * If dataType is BinaryVector/Float16Vector/BFloat16Vector/Int8Vector, use JsonObject.add(key, gson.toJsonTree(byte[])) to input;
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
     * A Float field named "XX", you set the value to be "3.5" by JsonObject.add("XX", "3.5"), the string "3.5" is parsed as 3.5.
     *
     */
    private List<JsonObject> data;
    private String databaseName;
    private String collectionName;
    private String partitionName;

    private String rlsPrincipal;
    private Boolean skipRls;

    private InsertReq(InsertReqBuilder builder) {
        this.data = builder.data;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.rlsPrincipal = builder.rlsPrincipal;
        this.skipRls = builder.skipRls;
    }

    public static InsertReqBuilder builder() {
        return new InsertReqBuilder();
    }

    public List<JsonObject> getData() {
        return data;
    }

    public void setData(List<JsonObject> data) {
        this.data = data;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    /**
     * Returns the row-level security (RLS) principal for the operation.
     *
     * @return the RLS principal
     */


    public String getRlsPrincipal() {
        return rlsPrincipal;
    }

    /**
     * Sets the row-level security (RLS) principal for the operation.
     *
     * @param rlsPrincipal the RLS principal
     */


    public void setRlsPrincipal(String rlsPrincipal) {
        this.rlsPrincipal = rlsPrincipal;
    }

    /**
     * Returns whether the operation should skip row-level security (RLS) enforcement.
     *
     * @return {@code true} to skip RLS, {@code null} for the server default
     */


    public Boolean getSkipRls() {
        return skipRls;
    }

    /**
     * Sets whether the operation should skip row-level security (RLS) enforcement.
     *
     * @param skipRls {@code true} to skip RLS, {@code null} for the server default
     */


    public void setSkipRls(Boolean skipRls) {
        this.skipRls = skipRls;
    }

    @Override
    public String toString() {
        return "InsertReq{" +
                "data=" + data +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", rlsPrincipal='" + rlsPrincipal + '\'' +
                ", skipRls=" + skipRls +
                '}';
    }

    public static class InsertReqBuilder {
        private List<JsonObject> data;
        private String databaseName = "";
        private String collectionName;
        private String partitionName = "";

        private String rlsPrincipal;
        private Boolean skipRls;

        public InsertReqBuilder data(List<JsonObject> data) {
            this.data = data;
            return this;
        }

        public InsertReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public InsertReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public InsertReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the row-level security (RLS) principal for the operation.
         *
         * @param rlsPrincipal the RLS principal
         * @return this builder
         */


        public InsertReqBuilder rlsPrincipal(String rlsPrincipal) {
            this.rlsPrincipal = rlsPrincipal;
            return this;
        }

        /**
         * Sets whether the operation should skip row-level security (RLS) enforcement.
         *
         * @param skipRls {@code true} to skip RLS, {@code null} for the server default
         * @return this builder
         */


        public InsertReqBuilder skipRls(Boolean skipRls) {
            this.skipRls = skipRls;
            return this;
        }

        public InsertReq build() {
            return new InsertReq(this);
        }
    }
}
