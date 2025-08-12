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

package io.milvus.bulkwriter.request.import_;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
/*
  If you want to import data into a Zilliz cloud instance and your data is stored in a Zilliz stage,
  you can use this method to import the data from the stage.
 */
public class StageImportRequest extends BaseImportRequest {
    private String clusterId;

    /**
     * For Free & Serverless deployments: specifying this parameter is not supported.
     * For Dedicated deployments: this parameter can be specified; defaults to the "default" database.
     */
    private String dbName;
    private String collectionName;

    /**
     * If the collection has partitionKey enabled:
     *     - The partitionName parameter cannot be specified for import.
     * If the collection does not have partitionKey enabled:
     *     - You may specify partitionName for the import.
     *     - Defaults to the "default" partition if not specified.
     */
    private String partitionName;

    private String stageName;

    /**
     * Data import can be configured in multiple ways using `dataPaths`:
     * <p>
     * 1. Multi-path import (multiple folders or files):
     *    "dataPaths": [
     *        ["parquet-folder-1/1.parquet"],
     *        ["parquet-folder-2/1.parquet"],
     *        ["parquet-folder-3/"]
     *    ]
     * <p>
     * 2. Folder import:
     *    "dataPaths": [
     *        ["parquet-folder/"]
     *    ]
     * <p>
     * 3. Single file import:
     *    "dataPaths": [
     *        ["parquet-folder/1.parquet"]
     *    ]
     */
    private List<List<String>> dataPaths;
}
