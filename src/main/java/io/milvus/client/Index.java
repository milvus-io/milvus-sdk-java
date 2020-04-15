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

/** Represents an index containing <code>indexType</code> and <code>nList</code> */
public class Index {
  private final String collectionName;
  private final IndexType indexType;
  private final String paramsInJson;

  private Index(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.indexType = builder.indexType;
    this.paramsInJson = builder.paramsInJson;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public String getParamsInJson() {
    return paramsInJson;
  }

  @Override
  public String toString() {
    return "Index {"
        + "collectionName="
        + collectionName
        + ", indexType="
        + indexType
        + ", params="
        + paramsInJson
        + '}';
  }

  /** Builder for <code>Index</code> */
  public static class Builder {
    // Required parameters
    private final String collectionName;
    private final IndexType indexType;

    // Optional parameters - initialized to default values
    private String paramsInJson;

    /**
     * @param collectionName collection to create index on
     * @param indexType a <code>IndexType</code> object
     */
    public Builder(@Nonnull String collectionName, @Nonnull IndexType indexType) {
      this.collectionName = collectionName;
      this.indexType = indexType;
    }

    /**
     * Optional. Default to empty <code>String</code>. Index parameters are different for different
     * index types. Refer to <a
     * href="https://milvus.io/docs/v0.7.0/guides/milvus_operation.md">https://milvus.io/docs/v0.7.0/guides/milvus_operation.md</a>
     * for more information.
     *
     * <pre>
     * FLAT/IVFLAT/SQ8: {"nlist": 16384}
     * nlist range:[1, 999999]
     *
     * IVFPQ: {"nlist": 16384, "m": 12}
     * nlist range:[1, 999999]
     * m is decided by dim and have a couple of results.
     *
     * NSG: {"search_length": 45, "out_degree": 50, "candidate_pool_size": 300, "knng": 100}
     * search_length range:[10, 300]
     * out_degree range:[5, 300]
     * candidate_pool_size range:[50, 1000]
     * knng range:[5, 300]
     *
     * HNSW: {"M": 16, "efConstruction": 500}
     * M range:[5, 48]
     * efConstruction range:[100, 500]
     *
     * ANNOY: {"n_trees": 4}
     * n_trees range: [1, 16384)
     * </pre>
     *
     * @param paramsInJson extra parameters in JSON format
     * @return <code>Builder</code>
     */
    public Builder withParamsInJson(@Nonnull String paramsInJson) {
      this.paramsInJson = paramsInJson;
      return this;
    }

    public Index build() {
      return new Index(this);
    }
  }
}
