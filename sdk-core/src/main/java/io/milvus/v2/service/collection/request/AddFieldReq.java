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

package io.milvus.v2.service.collection.request;

import io.milvus.v2.common.DataType;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@Data
@SuperBuilder
public class AddFieldReq {
    private String fieldName;
    @Builder.Default
    private String description = "";
    private DataType dataType;
    @Builder.Default
    private Integer maxLength = 65535;
    @Builder.Default
    private Boolean isPrimaryKey = Boolean.FALSE;
    @Builder.Default
    private Boolean isPartitionKey = Boolean.FALSE;
    @Builder.Default
    private Boolean isClusteringKey = Boolean.FALSE;
    @Builder.Default
    private Boolean autoID = Boolean.FALSE;
    private Integer dimension;
    private DataType elementType;
    private Integer maxCapacity;
    @Builder.Default
    private Boolean isNullable = Boolean.FALSE; // only for scalar fields(not include Array fields)
    @Builder.Default
    private Object defaultValue = null; // only for scalar fields
    @Builder.ObtainVia(field = "hiddenField")
    private boolean enableDefaultValue = false; // a flag to pass the default value to server or not
    private Boolean enableAnalyzer; // for BM25 tokenizer
    private Map<String, Object> analyzerParams; // for BM25 tokenizer
    private Boolean enableMatch; // for BM25 keyword search

    // If a specific field, such as maxLength, has been specified, it will override the corresponding key's value in typeParams.
    private Map<String, String> typeParams;
    private Map<String, Object> multiAnalyzerParams; // for multi‑language analyzers

    public static abstract class AddFieldReqBuilder<C extends AddFieldReq, B extends AddFieldReq.AddFieldReqBuilder<C, B>> {
        public B defaultValue(Object value) {
            this.defaultValue$value = value;
            this.defaultValue$set = true;

            this.enableDefaultValue = true; // automatically set this flag
            return self();
        }
    }
}
