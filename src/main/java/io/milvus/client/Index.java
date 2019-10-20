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
  private final IndexType indexType;
  private final int nList;

  private Index(@Nonnull Builder builder) {
    this.indexType = builder.indexType;
    this.nList = builder.nList;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public int getNList() {
    return nList;
  }

  @Override
  public String toString() {
    return "Index {" + "indexType=" + indexType + ", nList=" + nList + '}';
  }

  /** Builder for <code>Index</code> */
  public static class Builder {
    // Optional parameters - initialized to default values
    private IndexType indexType = IndexType.FLAT;
    private int nList = 16384;

    /**
     * Optional. Default to <code>IndexType.FLAT</code>
     *
     * @param indexType a <code>IndexType</code> object
     * @return <code>Builder</code>
     */
    public Builder withIndexType(@Nonnull IndexType indexType) {
      this.indexType = indexType;
      return this;
    }

    /**
     * Optional. Default to 16384.
     *
     * @param nList nList of the index
     * @return <code>Builder</code>
     */
    public Builder withNList(int nList) {
      this.nList = nList;
      return this;
    }

    public Index build() {
      return new Index(this);
    }
  }
}
