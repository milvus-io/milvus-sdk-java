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

/** Contains parameters for <code>search</code> */
public class SearchParam {

  private final String tableName;
  private final List<List<Float>> queryVectors;
  private final List<DateRange> dateRanges;
  private final long topK;
  private final long nProbe;
  private final List<String> partitionTags;

  private SearchParam(@Nonnull Builder builder) {
    this.tableName = builder.tableName;
    this.queryVectors = builder.queryVectors;
    this.dateRanges = builder.dateRanges;
    this.nProbe = builder.nProbe;
    this.topK = builder.topK;
    this.partitionTags = builder.partitionTags;
  }

  public String getTableName() {
    return tableName;
  }

  public List<List<Float>> getQueryVectors() {
    return queryVectors;
  }

  public List<DateRange> getDateRanges() {
    return dateRanges;
  }

  public long getTopK() {
    return topK;
  }

  public long getNProbe() {
    return nProbe;
  }

  public List<String> getPartitionTags() {
    return partitionTags;
  }

  /** Builder for <code>SearchParam</code> */
  public static class Builder {
    // Required parameters
    private final String tableName;
    private final List<List<Float>> queryVectors;

    // Optional parameters - initialized to default values
    private List<DateRange> dateRanges = new ArrayList<>();
    private long topK = 1024;
    private long nProbe = 20;
    private List<String> partitionTags = new ArrayList<>();

    /**
     * @param tableName table to search from
     * @param queryVectors a <code>List</code> of vectors to be queried. Each inner <code>List
     *     </code> represents a vector.
     */
    public Builder(@Nonnull String tableName, @Nonnull List<List<Float>> queryVectors) {
      this.tableName = tableName;
      this.queryVectors = queryVectors;
    }

    /**
     * Optional. Searches vectors in their corresponding date range. Default to an empty <code>
     * ArrayList</code>
     *
     * @param dateRanges a <code>List</code> of <code>DateRange</code> objects
     * @return <code>Builder</code>
     * @see DateRange
     */
    public Builder withDateRanges(@Nonnull List<DateRange> dateRanges) {
      this.dateRanges = dateRanges;
      return this;
    }

    /**
     * Optional. Limits search result to <code>topK</code>. Default to 1024.
     *
     * @param topK a topK number
     * @return <code>Builder</code>
     */
    public Builder withTopK(long topK) {
      this.topK = topK;
      return this;
    }

    /**
     * Optional. Default to 20.
     *
     * @param nProbe a nProbe number
     * @return <code>Builder</code>
     */
    public Builder withNProbe(long nProbe) {
      this.nProbe = nProbe;
      return this;
    }

    /**
     * Optional. Search vectors with corresponding <code>partitionTags</code>. Default to an empty <code>List</code>
     *
     * @param partitionTags a <code>List</code> of partition tags
     * @return <code>Builder</code>
     */
    public Builder withPartitionTags(List<String> partitionTags) {
      this.partitionTags = partitionTags;
      return this;
    }

    public SearchParam build() {
      return new SearchParam(this);
    }
  }
}
