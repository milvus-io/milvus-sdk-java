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

import com.alibaba.fastjson.JSONObject;
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
        private List<JSONObject> rows;

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
         * @param rows insert row data
         * @return <code>Builder</code>
         * @see JSONObject
         */
        public Builder withRows(@NonNull List<JSONObject> rows) {
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
