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

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.*;

@Data
@SuperBuilder
public class RunAnalyzerReq {
    @Builder.Default
    private List<String> texts = new ArrayList<>();
    @Builder.Default
    private Map<String, Object> analyzerParams = new HashMap<>();
    @Builder.Default
    private Boolean withDetail = Boolean.FALSE;
    @Builder.Default
    private Boolean withHash = Boolean.FALSE;
    @Builder.Default
    private String databaseName = "";
    @Builder.Default
    private String collectionName = "";
    @Builder.Default
    private String fieldName = "";
    @Builder.Default
    private List<String> analyzerNames = new ArrayList<>();
}
