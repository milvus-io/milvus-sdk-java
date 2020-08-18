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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/** Contains parameters for <code>search</code> */
public class SearchParam {

  private final String collectionName;
  private final String dsl;
  private final Map<String, List<ByteBuffer>> binaryEntities;
  private final List<String> partitionTags;
  private final String paramsInJson;

  private SearchParam(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.dsl = builder.dsl;
    this.partitionTags = builder.partitionTags;
    this.paramsInJson = builder.paramsInJson;
    this.binaryEntities = builder.binaryEntities;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public String getDSL() { return dsl; }

  public Map<String, List<ByteBuffer>> getBinaryEntities() { return binaryEntities; }

  public List<String> getPartitionTags() {
    return partitionTags;
  }

  public String getParamsInJson() {
    return paramsInJson;
  }

  /** Builder for <code>SearchParam</code> */
  public static class Builder {
    // Required parameter
    private final String collectionName;

    // Optional parameters - initialized to default values
    private List<String> partitionTags = new ArrayList<>();
    private String dsl = "{}";
    private String paramsInJson = "{}";
    private Map<String, List<ByteBuffer>> binaryEntities = new HashMap<>();

    /** @param collectionName collection to search from */
    public Builder(@Nonnull String collectionName) {
      this.collectionName = collectionName;
    }

    /**
     * The DSL statement for search. DSL provides a more convenient and idiomatic way to write and
     * manipulate queries. It is in JSON format (passed into builder as String), and an example of
     * DSL statement is as follows.
     *
     * <pre>
     *   <code>
     * {
     *     "bool": {
     *         "must": [
     *             {
     *                 "term": {
     *                     "A": [1, 2, 5]
     *                 }
     *             },
     *             {
     *                 "range": {
     *                     "B": {"GT": 1, "LT": 100}
     *                 }
     *             },
     *             {
     *                 "vector": {
     *                     "Vec": {
     *                         "topk": 10, "type": "float", "query": list_of_vecs, "params": {"nprobe": 10}
     *                     }
     *                 }
     *             }
     *         ],
     *     },
     * }
     *   </code>
     * </pre>
     *
     * Note that "vector" must be included in DSL. The "params" in "Vec" is different for different
     * index types. Refer to Milvus documentation for more information about DSL.
     *
     * A "type" key must be present in "Vec" field to indicate whether your query vectors are
     * "float" or "binary".
     *
     * @param dsl The DSL String in JSON format
     * @return <code>Builder</code>
     */
    public SearchParam.Builder withDSL(@Nonnull String dsl) {
      this.dsl = dsl;
      return this;
    }

    /**
     * Optional. Default to empty map. Due to the nature of <code>ByteBuffer</code>, it is not
     * feasible to pass binary entities as query vectors in DSL statement.
     * JSON strings cannot be parsed back to <code>List<ByteBuffer></code> object.
     *
     * The map will take user-defined name (placeholder) as the key, and list of query vectors as
     * the value. When building DSL statement, use the placeholder instead of raw entities in "query".
     * For example, for float vectors we have
     * <code>
     *   {"topk": 10, "type": "float", "query": vecs, "params": {"nprobe": 10}}
     * </code>
     * While for binary vectors we have
     * <code>
     *   {"topk": 10, "type": "binary", "query": "placeholder", "params": {"nprobe": 10}}
     * </code>
     * And in <code>binaryEntities</code>, we have a key-value pair of ("placeholder", vecs).
     *
     * @param binaryEntities a <code>Map</code> of placeholders to query binary vectors. If using
     *                       float data, this builder is not needed.
     * @return <code>Builder</code>
     */
    public SearchParam.Builder withBinaryEntities(@Nonnull Map<String, List<ByteBuffer>> binaryEntities) {
      this.binaryEntities = binaryEntities;
      return this;
    }

    /**
     * Optional. Search vectors with corresponding <code>partitionTags</code>. Default to an empty
     * <code>List</code>
     *
     * @param partitionTags a <code>List</code> of partition tags
     * @return <code>Builder</code>
     */
    public SearchParam.Builder withPartitionTags(@Nonnull List<String> partitionTags) {
      this.partitionTags = partitionTags;
      return this;
    }

    /**
     * Optional. Default to empty <code>String</code>. This is to specify the fields you would like
     * Milvus server to return from query results. No field information will be returned if this
     * is not specified.
     *
     * <pre>
     *   {"fields": ["B", "D"]}
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
