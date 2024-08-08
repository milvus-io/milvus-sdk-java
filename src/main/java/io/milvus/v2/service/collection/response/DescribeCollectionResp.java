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

package io.milvus.v2.service.collection.response;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class DescribeCollectionResp {
    private String collectionName;
    private String databaseName;
    private String description;
    private Long numOfPartitions;

    private List<String> fieldNames;
    private List<String> vectorFieldNames;
    private String primaryFieldName;
    private Boolean enableDynamicField;
    private Boolean autoID;

    private CreateCollectionReq.CollectionSchema collectionSchema;
    private Long createTime;
    private ConsistencyLevel consistencyLevel;
    @Builder.Default
    private final Map<String, String> properties = new HashMap<>();
}
