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
 * Represents different types of indexing method to query the table:
 *
 * <pre>
 *
 * 1. FLAT - Provides 100% accuracy for recalls. However, performance might be downgraded due to huge computation effort;
 *
 * 2. IVFLAT - K-means based similarity search which is balanced between accuracy and performance;
 *
 * 3. IVF_SQ8 - Vector indexing that adopts a scalar quantization strategy that significantly reduces the size of a
 * vector (by about 3/4), thus improving the overall throughput of vector processing;
 *
 * 4. NSG - NSG (Navigating Spreading-out Graph) is a graph-base search algorithm that a) lowers the average
 * out-degree of the graph for fast traversal; b) shortens the search path; c) reduces the index
 * size; d) lowers the indexing complexity. Extensive tests show that NSG can achieve very high
 * search performance at high precision, and needs much less memory. Compared to non-graph-based
 * algorithms, it is faster to achieve the same search precision.
 *
 * 5. IVF_SQ8_H - An enhanced index algorithm of IVF_SQ8. It supports hybrid computation on both CPU and GPU,
 * which significantly improves the search performance. To use this index type, make sure both cpu and gpu are added as
 * resources for search usage in the Milvus configuration file.
 *
 * </pre>
 */
public enum IndexType {
  INVALID(0),
  FLAT(1),
  IVFLAT(2),
  IVF_SQ8(3),
  NSG(4),
  IVF_SQ8_H(5),

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
