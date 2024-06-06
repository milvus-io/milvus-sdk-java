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

package io.milvus.param.highlevel.dml;

import com.google.gson.JsonObject;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import io.milvus.param.dml.InsertParam;
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
public class InsertRowsParam {

    private final InsertParam insertParam;
    private InsertRowsParam(InsertParam insertParam) {
        this.insertParam = insertParam;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link InsertRowsParam} class.
     */
    public static class Builder {
        private String collectionName;
        private List<JsonObject> rows;

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
         * Sets the row data to insert. The rows list cannot be empty.
         *
         * Internal class for insert data.
         * If dataType is Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar, use JsonObject.addProperty(key, value) to input;
         * If dataType is FloatVector, use JsonObject.add(key, gson.toJsonTree(List[Float])) to input;
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
         * Verifies parameters and creates a new {@link InsertRowsParam} instance.
         *
         * @return {@link InsertRowsParam}
         */
        public InsertRowsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (CollectionUtils.isEmpty(rows)) {
                throw new ParamException("Rows cannot be empty");
            }

            // this method doesn't check data type, the insert() api will do this work
            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withRows(rows)
                    .build();
            return new InsertRowsParam(insertParam);
        }
    }
}
