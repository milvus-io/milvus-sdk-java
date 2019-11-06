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

import java.util.List;

/**
 * Contains the returned <code>response</code> and <code>tableNames</code> for <code>showTables
 * </code>
 */
public class ShowTablesResponse {
  private final Response response;
  private final List<String> tableNames;

  public ShowTablesResponse(Response response, List<String> tableNames) {
    this.response = response;
    this.tableNames = tableNames;
  }

  public List<String> getTableNames() {
    return tableNames;
  }

  public Response getResponse() {
    return response;
  }

  public boolean ok() {
    return response.ok();
  }

  @Override
  public String toString() {
    return String.format(
        "ShowTablesResponse {%s, table names = %s}", response, tableNames.toString());
  }
}
