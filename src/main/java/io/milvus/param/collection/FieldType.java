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

import io.milvus.grpc.DataType;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Field schema for collection
 *
 * @author changzechuan
 */
public class FieldType {
    private final long fieldID;
    private final String name;
    private final boolean primaryKey;
    private final String description;
    private final DataType dataType;
    private final Map<String,String> typeParams;
    private final Map<String,String> indexParams;
    private final boolean autoID;

    private FieldType(@Nonnull Builder builder){
        this.fieldID = builder.fieldID;
        this.name = builder.name;
        this.primaryKey = builder.primaryKey;
        this.description = builder.description;
        this.dataType = builder.dataType;
        this.typeParams = builder.typeParams;
        this.indexParams = builder.indexParams;
        this.autoID = builder.autoID;
    }

    public long getFieldID() {
        return fieldID;
    }

    public String getName() {
        return name;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String getDescription() {
        return description;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    public Map<String, String> getIndexParams() {
        return indexParams;
    }

    public boolean isAutoID() {
        return autoID;
    }

    public static final class Builder {
        private long fieldID;
        private String name;
        private boolean primaryKey;
        private String description;
        private DataType dataType;
        private Map<String,String> typeParams;
        private Map<String,String> indexParams;
        private boolean autoID;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withFieldID(long fieldID) {
            this.fieldID = fieldID;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withDataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder withTypeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
            return this;
        }

        public Builder withIndexParams(Map<String, String> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        public Builder withAutoID(boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        public FieldType build() {
            return new FieldType(this);
        }
    }
}
