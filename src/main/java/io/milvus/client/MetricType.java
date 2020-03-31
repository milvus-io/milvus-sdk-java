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

/** Represents available metric types */
public enum MetricType {
  INVALID(0),
  /** Euclidean distance. For float vectors only */
  L2(1),
  /** Inner product. For float vectors only */
  IP(2),
  /** Hamming distance. For binary vectors only */
  HAMMING(3),
  /** Jaccard distance. For binary vectors only */
  JACCARD(4),
  /** Tanimoto distance. For binary vectors only */
  TANIMOTO(5),
  /** Substructure: D(a, b) = 1 - N(a&b) / N(b). For binary vectors only */
  SUBSTRUCTURE(6),
  /** Superstructure: D(a, b) = 1 - N(a&b) / N(a). For binary vectors only */
  SUPERSTRUCTURE(7),

  UNKNOWN(-1);

  private final int val;

  MetricType(int val) {
    this.val = val;
  }

  public static MetricType valueOf(int val) {
    Optional<MetricType> search =
        Arrays.stream(values()).filter(metricType -> metricType.val == val).findFirst();
    return search.orElse(UNKNOWN);
  }

  public int getVal() {
    return val;
  }
}
