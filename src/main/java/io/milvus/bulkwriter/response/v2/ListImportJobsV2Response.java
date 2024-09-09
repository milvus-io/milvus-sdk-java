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

import com.google.common.collect.ImmutableMap;
import io.milvus.bulkwriter.response.ListImportJobsResponse;
import io.milvus.bulkwriter.response.Record;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ListImportJobsV2Response implements Serializable {

    private static final long serialVersionUID = -8400893490624599225L;

    private Integer count;

    private Integer currentPage;

    private Integer pageSize;

    private List<Record> records;

    public ListImportJobsResponse toListImportJobsResponse() {
        Map<String, String> newOldStateMap = ImmutableMap.of(
                "Pending","ImportPending",
                "Importing","ImportRunning",
                "Completed","ImportCompleted",
                "Failed","ImportFailed",
                "Cancel","ImportCancel"
        );

        List<Record> tasks = new ArrayList<>();
        for (Record record : records) {
            Record task = Record.builder()
                    .jobId(record.getJobId())
                    .collectionName(record.getCollectionName())
                    .state(newOldStateMap.get(record.getState()))
                    .build();
            tasks.add(task);
        }

        ListImportJobsResponse listImportJobsResponse = new ListImportJobsResponse();
        listImportJobsResponse.setCount(count);
        listImportJobsResponse.setCurrentPage(currentPage);
        listImportJobsResponse.setPageSize(pageSize);
        listImportJobsResponse.setTasks(tasks);
        return listImportJobsResponse;
    }
}
