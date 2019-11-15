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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/** Contains parameters for <code>insert</code> */
public class InsertParam {
  private final String tableName;
  private final List<List<Float>> vectors;
  private final List<Long> vectorIds;
  private final String partitionTag;

  private InsertParam(@Nonnull Builder builder) {
    this.tableName = builder.tableName;
    this.vectors = builder.vectors;
    this.vectorIds = builder.vectorIds;
    this.partitionTag = builder.partitionTag;
  }

  public String getTableName() {
    return tableName;
  }

  public List<List<Float>> getVectors() {
    return vectors;
  }

  public List<Long> getVectorIds() {
    return vectorIds;
  }

  public String getPartitionTag() {
    return partitionTag;
  }

  /** Builder for <code>InsertParam</code> */
  public static class Builder {
    // Required parameters
    private final String tableName;
    private final List<List<Float>> vectors;

    // Optional parameters - initialized to default values
    private List<Long> vectorIds = new ArrayList<>();
    private String partitionTag = "";

    /**
     * @param tableName table to insert vectors to
     * @param vectors a <code>List</code> of vectors to insert. Each inner <code>List</code>
     *     represents a vector.
     */
    public Builder(@Nonnull String tableName, @Nonnull List<List<Float>> vectors) {
      this.tableName = tableName;
      this.vectors = vectors;
    }

    /**
     * Optional. Default to an empty <code>ArrayList</code>
     *
     * @param vectorIds a <code>List</code> of ids associated with the vectors to insert
     * @return <code>Builder</code>
     */
    public Builder withVectorIds(@Nonnull List<Long> vectorIds) {
      this.vectorIds = vectorIds;
      return this;
    }

    /**
     * Optional. Default to an empty <code>String</code>
     *
     * @param partitionTag partition tag
     * @return <code>Builder</code>
     */
    public Builder withPartitionTag(@Nonnull String partitionTag) {
      this.partitionTag = partitionTag;
      return this;
    }

    public InsertParam build() {
      return new InsertParam(this);
    }
  }
}
