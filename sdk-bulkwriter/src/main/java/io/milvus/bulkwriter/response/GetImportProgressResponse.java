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

package io.milvus.bulkwriter.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GetImportProgressResponse implements Serializable {

    private static final long serialVersionUID = -2302203037749197132L;

    private String jobId;

    private String collectionName;

    private String fileName;

    private Integer fileSize;

    private String state;

    private Integer progress;

    private String completeTime;

    private String reason;

    private Integer totalRows;

    private List<Detail> details;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Detail {
        private String fileName;
        private Integer fileSize;
        private String state;
        private Integer progress;
        private String completeTime;
        private String reason;
    }
}
