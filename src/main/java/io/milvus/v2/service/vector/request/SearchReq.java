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
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class SearchReq {
    private String collectionName;
    @Builder.Default
    private List<String> partitionNames = new ArrayList<>();
    @Builder.Default
    private String annsField = "";
    private IndexParam.MetricType metricType;
    private int topK;
    private String filter;
    @Builder.Default
    private List<String> outputFields = new ArrayList<>();
    private List<BaseVector> data;
    private long offset;
    private long limit;

    @Builder.Default
    private int roundDecimal = -1;
    @Builder.Default
    private Map<String, Object> searchParams = new HashMap<>();
    private long guaranteeTimestamp; // deprecated
    @Builder.Default
    private Long gracefulTime = 5000L; // deprecated
    @Builder.Default
    private ConsistencyLevel consistencyLevel = null;
    private boolean ignoreGrowing;
    private String groupByFieldName;
    private Integer groupSize;
    private Boolean strictGroupSize;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    @Builder.Default
    private Map<String, Object> filterTemplateValues = new HashMap<>();
}
