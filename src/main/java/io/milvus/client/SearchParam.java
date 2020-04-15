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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Contains parameters for <code>search</code> */
public class SearchParam {

  private final String collectionName;
  private final List<List<Float>> floatVectors;
  private final List<ByteBuffer> binaryVectors;
  private final List<String> partitionTags;
  private final long topK;
  private final String paramsInJson;

  private SearchParam(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.floatVectors = builder.floatVectors;
    this.binaryVectors = builder.binaryVectors;
    this.partitionTags = builder.partitionTags;
    this.topK = builder.topK;
    this.paramsInJson = builder.paramsInJson;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public List<List<Float>> getFloatVectors() {
    return floatVectors;
  }

  public List<ByteBuffer> getBinaryVectors() {
    return binaryVectors;
  }

  public List<String> getPartitionTags() {
    return partitionTags;
  }

  public long getTopK() {
    return topK;
  }

  public String getParamsInJson() {
    return paramsInJson;
  }

  /** Builder for <code>SearchParam</code> */
  public static class Builder {
    // Required parameters
    private final String collectionName;

    // Optional parameters - initialized to default values
    private List<List<Float>> floatVectors = new ArrayList<>();
    private List<ByteBuffer> binaryVectors = new ArrayList<>();
    private List<String> partitionTags = new ArrayList<>();
    private long topK = 1024;
    private String paramsInJson;

    /** @param collectionName collection to search from */
    public Builder(@Nonnull String collectionName) {
      this.collectionName = collectionName;
    }

    /**
     * Default to an empty <code>ArrayList</code>. You can search either float or binary vectors,
     * not both.
     *
     * @param floatVectors a <code>List</code> of float vectors to be queries. Each inner <code>List
     *     </code> represents a float vector.
     * @return <code>Builder</code>
     */
    public SearchParam.Builder withFloatVectors(@Nonnull List<List<Float>> floatVectors) {
      this.floatVectors = floatVectors;
      return this;
    }

    /**
     * Default to an empty <code>ArrayList</code>. You can search either float or binary vectors,
     * not both.
     *
     * @param binaryVectors a <code>List</code> of binary vectors to be queried. Each <code>
     *     ByteBuffer</code> object represents a binary vector, with every 8 bits constituting a
     *     byte.
     * @return <code>Builder</code>
     * @see ByteBuffer
     */
    public SearchParam.Builder withBinaryVectors(@Nonnull List<ByteBuffer> binaryVectors) {
      this.binaryVectors = binaryVectors;
      return this;
    }

    /**
     * Optional. Search vectors with corresponding <code>partitionTags</code>. Default to an empty
     * <code>List</code>
     *
     * @param partitionTags a <code>List</code> of partition tags
     * @return <code>Builder</code>
     */
    public Builder withPartitionTags(@Nonnull List<String> partitionTags) {
      this.partitionTags = partitionTags;
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
     * Optional. Default to empty <code>String</code>. Search parameters are different for different
     * index types. Refer to <a
     * href="https://milvus.io/docs/v0.7.0/guides/milvus_operation.md">https://milvus.io/docs/v0.7.0/guides/milvus_operation.md</a>
     * for more information.
     *
     * <pre>
     *   FLAT/IVFLAT/SQ8/IVFPQ: {"nprobe": 32}
     *   nprobe range:[1,999999]
     *
     *   NSG: {"search_length": 100}
     *   search_length range:[10, 300]
     *
     *   HNSW: {"ef": 64}
     *   ef range:[topk, 4096]
     *
     *   ANNOY: {search_k", 0.05 * totalDataCount}
     *   search_k range: none
     * </pre>
     *
     * @param paramsInJson extra parameters in JSON format
     * @return <code>Builder</code>
     */
    public SearchParam.Builder withParamsInJson(@Nonnull String paramsInJson) {
      this.paramsInJson = paramsInJson;
      return this;
    }

    public SearchParam build() {
      return new SearchParam(this);
    }
  }
}
