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

/** Represents a collection mapping */
// Builder Pattern
public class CollectionMapping {
  private final String collectionName;
  private final long dimension;
  private final long indexFileSize;
  private final MetricType metricType;

  private CollectionMapping(@Nonnull Builder builder) {
    collectionName = builder.collectionName;
    dimension = builder.dimension;
    indexFileSize = builder.indexFileSize;
    metricType = builder.metricType;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public long getDimension() {
    return dimension;
  }

  public long getIndexFileSize() {
    return indexFileSize;
  }

  public MetricType getMetricType() {
    return metricType;
  }

  @Override
  public String toString() {
    return String.format(
        "CollectionMapping = {collectionName = %s, dimension = %d, indexFileSize = %d, metricType = %s}",
        collectionName, dimension, indexFileSize, metricType.name());
  }

  /** Builder for <code>CollectionMapping</code> */
  public static class Builder {
    // Required parameters
    private final String collectionName;
    private final long dimension;

    // Optional parameters - initialized to default values
    private long indexFileSize = 1024;
    private MetricType metricType = MetricType.L2;

    /**
     * @param collectionName collection name
     * @param dimension vector dimension
     */
    public Builder(@Nonnull String collectionName, long dimension) {
      this.collectionName = collectionName;
      this.dimension = dimension;
    }

    /**
     * Optional. Default to 1024 MB.
     *
     * @param indexFileSize in megabytes.
     * @return <code>Builder</code>
     */
    public Builder withIndexFileSize(long indexFileSize) {
      this.indexFileSize = indexFileSize;
      return this;
    }

    /**
     * Optional. Default to MetricType.L2
     *
     * @param metricType a <code>MetricType</code> value
     * @return <code>Builder</code>
     * @see MetricType
     */
    public Builder withMetricType(@Nonnull MetricType metricType) {
      this.metricType = metricType;
      return this;
    }

    public CollectionMapping build() {
      return new CollectionMapping(this);
    }
  }
}
