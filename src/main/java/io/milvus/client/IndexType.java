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

import java.util.Arrays;
import java.util.Optional;

/**
 * Represents different types of indexing method to query the collection. Refer to <a
 * href="https://milvus.io/docs/v0.8.0/guides/index.md">https://milvus.io/docs/v0.8.0/guides/index.md</a>
 * for more information.
 */
public enum IndexType {
  INVALID(0),
  FLAT(1),
  IVFLAT(2),
  IVF_SQ8(3),
  RNSG(4),
  IVF_SQ8_H(5),
  IVF_PQ(6),
  HNSW(11),
  ANNOY(12),

  UNKNOWN(-1);

  private final int val;

  IndexType(int val) {
    this.val = val;
  }

  public static IndexType valueOf(int val) {
    Optional<IndexType> search =
        Arrays.stream(values()).filter(indexType -> indexType.val == val).findFirst();
    return search.orElse(UNKNOWN);
  }

  public int getVal() {
    return val;
  }
}
