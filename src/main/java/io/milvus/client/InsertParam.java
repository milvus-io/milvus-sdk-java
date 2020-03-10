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

/** Contains parameters for <code>insert</code> */
public class InsertParam {
  private final String collectionName;
  private final List<List<Float>> floatVectors;
  private final List<ByteBuffer> binaryVectors;
  private final List<Long> vectorIds;
  private final String partitionTag;

  private InsertParam(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.floatVectors = builder.floatVectors;
    this.binaryVectors = builder.binaryVectors;
    this.vectorIds = builder.vectorIds;
    this.partitionTag = builder.partitionTag;
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

  public List<Long> getVectorIds() {
    return vectorIds;
  }

  public String getPartitionTag() {
    return partitionTag;
  }

  /** Builder for <code>InsertParam</code> */
  public static class Builder {
    // Required parameters
    private final String collectionName;

    // Optional parameters - initialized to default values
    private List<List<Float>> floatVectors = new ArrayList<>();
    private List<ByteBuffer> binaryVectors = new ArrayList<>();
    private List<Long> vectorIds = new ArrayList<>();
    private String partitionTag = "";

    /** @param collectionName collection to insert vectors to */
    public Builder(@Nonnull String collectionName) {
      this.collectionName = collectionName;
    }

    /**
     * Default to an empty <code>ArrayList</code>. You can only insert either float or binary
     * vectors to a collection, not both.
     *
     * @param floatVectors a <code>List</code> of float vectors to insert. Each inner <code>List
     *     </code> represents a float vector.
     * @return <code>Builder</code>
     */
    public Builder withFloatVectors(@Nonnull List<List<Float>> floatVectors) {
      this.floatVectors = floatVectors;
      return this;
    }

    /**
     * Default to an empty <code>ArrayList</code>. You can only insert either float or binary
     * vectors to a collection, not both.
     *
     * @param binaryVectors a <code>List</code> of binary vectors to insert. Each <code>ByteBuffer
     *     </code> objects represents a binary vector, with every 8 bits constituting a byte.
     * @return <code>Builder</code>
     * @see ByteBuffer
     */
    public Builder withBinaryVectors(@Nonnull List<ByteBuffer> binaryVectors) {
      this.binaryVectors = binaryVectors;
      return this;
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
