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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class InsertReq {
    //private List<> fields;

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
     */
    private List<JsonObject> data;
    private String databaseName;
    private String collectionName;
    private String partitionName;

    private InsertReq(Builder builder) {
        this.data = builder.data;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
    }

    public static Builder builder() {
        return new Builder();
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        InsertReq that = (InsertReq) obj;
        return new EqualsBuilder()
                .append(data, that.data)
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(partitionName, that.partitionName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(data)
                .append(databaseName)
                .append(collectionName)
                .append(partitionName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "InsertReq{" +
                "data=" + data +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }

    public static class Builder {
        private List<JsonObject> data;
        private String databaseName = "";
        private String collectionName;
        private String partitionName = "";

        public Builder data(List<JsonObject> data) {
            this.data = data;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public InsertReq build() {
            return new InsertReq(this);
        }
    }
}
