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

package io.milvus.bulkwriter.response.v2;

import io.milvus.bulkwriter.response.GetImportProgressResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GetImportProgressV2Response implements Serializable {

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

    private List<DetailV2> details;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class DetailV2 {
        private String fileName;
        private Integer fileSize;
        private String state;
        private Integer progress;
        private String completeTime;
        private String reason;


        public GetImportProgressResponse.Detail toDetail() {
            GetImportProgressResponse.Detail detail = new GetImportProgressResponse.Detail();
            detail.setFileName(fileName);
            detail.setFileSize(fileSize);
            detail.setReadyPercentage(progress == null ? null : Double.valueOf(progress));
            detail.setErrorMessage(reason);
            detail.setCompleteTime(completeTime);
            return detail;
        }
    }

    public GetImportProgressResponse toGetImportProgressResponse() {
        GetImportProgressResponse response = new GetImportProgressResponse();
        response.setJobId(jobId);
        response.setCollectionName(collectionName);
        response.setFileName(fileName);
        response.setFileSize(fileSize);
        response.setReadyPercentage(progress == null ? null : Double.valueOf(progress));
        response.setCompleteTime(completeTime);
        response.setErrorMessage(reason);

        List<GetImportProgressResponse.Detail> details = this.details.stream().map(DetailV2::toDetail).collect(Collectors.toList());
        response.setDetails(details);
        return response;
    }

}
