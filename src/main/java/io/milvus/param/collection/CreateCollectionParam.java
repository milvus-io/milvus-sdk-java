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

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * request for create collection
 *
 * @author changzechuan
 */
public class CreateCollectionParam {
    private final String collectionName;
    private final int shardsNum;
    private final String description;
    private final FieldType[] fieldTypes;

    private CreateCollectionParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.shardsNum = builder.shardsNum;
        this.description = builder.description;
        this.fieldTypes = builder.fieldTypes;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public int getShardsNum() {
        return shardsNum;
    }

    public String getDescription() {
        return description;
    }

    public FieldType[] getFieldTypes() {
        return fieldTypes;
    }


    public static final class Builder {
        private String collectionName;
        private int shardsNum = 2;
        private String description = "";
        private FieldType[] fieldTypes;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withShardsNum(int shardsNum) {
            this.shardsNum = shardsNum;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withFieldTypes(FieldType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public CreateCollectionParam build() {
            return new CreateCollectionParam(this);
        }
    }

    @Override
    public String toString() {
        return "CreateCollectionParam{" +
                "collectionName='" + collectionName + '\'' +
                ", shardsNum=" + shardsNum +
                ", description='" + description + '\'' +
                ", fieldTypes=" + Arrays.toString(fieldTypes) +
                '}';
    }
}
