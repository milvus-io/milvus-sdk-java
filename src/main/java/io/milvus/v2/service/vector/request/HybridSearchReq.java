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

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.ranker.BaseRanker;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class HybridSearchReq
{
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private List<AnnSearchReq> searchRequests;
    private BaseRanker ranker;
    private int topK;
    private List<String> outFields;
    @Builder.Default
    private int roundDecimal = -1;
    @Builder.Default
    private ConsistencyLevel consistencyLevel = null;

    private String groupByFieldName;
    private Integer groupSize;
    private Boolean strictGroupSize;
}
