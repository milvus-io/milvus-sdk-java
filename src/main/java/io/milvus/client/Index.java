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

/** Represents an index containing <code>fieldName</code>, <code>indexName</code> and
 * <code>paramsInJson</code>, which contains index_type, params etc.
 */
public class Index {
  private final String collectionName;
  private final String fieldName;
  private final String indexName;
  private final String paramsInJson;

  private Index(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.fieldName = builder.fieldName;
    this.indexName = builder.indexName;
    this.paramsInJson = builder.paramsInJson;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getParamsInJson() {
    return paramsInJson;
  }

  @Override
  public String toString() {
    return "Index {"
        + "collectionName="
        + collectionName
        + ", fieldName="
        + fieldName
        + ", params="
        + paramsInJson
        + '}';
  }

  /** Builder for <code>Index</code> */
  public static class Builder {
    // Required parameters
    private final String collectionName;
    private final String fieldName;

    // Optional parameters - initialized to default values
    private String paramsInJson = "{}";
    private String indexName = "";

    /**
     * @param collectionName collection to create index for
     * @param fieldName name of the field on which index is built. If set to empty string
     *                  in <code>dropIndex</code>, all index of the collection will be dropped.
     */
    public Builder(@Nonnull String collectionName, @Nonnull String fieldName) {
      this.collectionName = collectionName;
      this.fieldName = fieldName;
    }

    /**
     * Optional. The parameters for building an index. Index parameters are different for different
     * index types. Refer to <a
     * href="https://milvus.io/docs/v0.10.1/create_drop_index_python.md">https://milvus.io/docs/v0.10.1/create_drop_index_python.md</a>
     * for more information.
     *
     * Example index parameters in json format
     *    for vector field:
     *        extra_params["index_type"] = one of the values: FLAT, IVF_FLAT, IVF_SQ8, NSG,
     *                                                        IVF_SQ8_HYBRID, IVF_PQ, HNSW,
     *                                                        RHNSW_FLAT, RHNSW_PQ, RHNSW_SQ, ANNOY
     *        extra_params["metric_type"] = one of the values: L2, IP, HAMMING, JACCARD, TANIMOTO
     *                                                         SUBSTRUCTURE, SUPERSTRUCTURE
     *        extra_params["params"] = optional parameters for index, including <code>nlist</code>
     *
     * Example param: <code>
     *   {\"index_type\": "IVF_FLAT",
     *   \"metric_type\": "IP",
     *   \"params\": {\"nlist\": 2048}}
     * </code>
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
