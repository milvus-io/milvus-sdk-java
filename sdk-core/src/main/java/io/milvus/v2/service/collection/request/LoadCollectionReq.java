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

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class LoadCollectionReq {
    private String collectionName;
    @Builder.Default
    private Integer numReplicas = 1;
    @Builder.Default
    private Boolean async = Boolean.TRUE;
    @Builder.Default
    private Long timeout = 60000L;
    @Builder.Default
    private Boolean refresh = Boolean.FALSE;
    @Builder.Default
    private List<String> loadFields = new ArrayList<>();
    @Builder.Default
    private Boolean skipLoadDynamicField = Boolean.FALSE;
}
