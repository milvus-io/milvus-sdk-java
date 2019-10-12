/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

import javax.annotation.Nonnull;

/** Contains parameters for <code>createTable</code> */
public class TableSchemaParam {
  private final TableSchema tableSchema;
  private final long timeout;

  private TableSchemaParam(@Nonnull Builder builder) {
    this.tableSchema = builder.tableSchema;
    this.timeout = builder.timeout;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public long getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return "CreateTableParam {" + tableSchema + ", timeout = " + timeout + '}';
  }

  /** Builder for <code>TableSchemaParam</code> */
  public static class Builder {
    // Required parameters
    private final TableSchema tableSchema;

    // Optional parameters - initialized to default values
    private long timeout = 86400;

    /**
     * @param tableSchema a <code>TableSchema</code> object
     * @see TableSchema
     */
    public Builder(@Nonnull TableSchema tableSchema) {
      this.tableSchema = tableSchema;
    }

    /**
     * Optional. Sets the deadline from when the client RPC is set to when the response is picked up
     * by the client. Default to 86400s (1 day).
     *
     * @param timeout in seconds
     * @return <code>Builder</code>
     */
    public Builder withTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public TableSchemaParam build() {
      return new TableSchemaParam(this);
    }
  }
}
