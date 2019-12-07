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

package io.milvus.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains the returned <code>response</code> and <code>partitionList</code> for <code>showPartitions
 * </code>
 */
public class ShowPartitionsResponse {
  private final Response response;
  private final List<Partition> partitionList;

  ShowPartitionsResponse(Response response, List<Partition> partitionList) {
    this.response = response;
    this.partitionList = partitionList;
  }

  /**
   * @return a <code>List</code> of <code>Partition</code>s.
   */
  public List<Partition> getPartitionList() {
    return partitionList;
  }

  /**
   * @return a <code>List</code> of table names of each partition.
   */
  public List<String> getTableNameList() {
    List<String> tableNameList = new ArrayList<>();
    for (Partition partition : partitionList) {
      tableNameList.add(partition.getTableName());
    }
    return tableNameList;
  }

  /**
   * @return a <code>List</code> of partition names.
   */
  public List<String> getPartitionNameList() {
    List<String> partitionNameList = new ArrayList<>();
    for (Partition partition : partitionList) {
      partitionNameList.add(partition.getPartitionName());
    }
    return partitionNameList;
  }

  /**
   * @return a <code>List</code> of partition tags.
   */
  public List<String> getPartitionTagList() {
    List<String> partitionTagList = new ArrayList<>();
    for (Partition partition : partitionList) {
      partitionTagList.add(partition.getTag());
    }
    return partitionTagList;
  }

  public Response getResponse() {
    return response;
  }

  public boolean ok() {
    return response.ok();
  }
}
