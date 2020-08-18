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

/** Contains parameters for <code>compact</code> */
public class CompactParam {
  private final String collectionName;
  private final double threshold;

  private CompactParam(@Nonnull Builder builder) {
    this.collectionName = builder.collectionName;
    this.threshold = builder.threshold;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public double getThreshold() {
    return threshold;
  }

  /** Builder for <code>CompactParam</code> */
  public static class Builder {
    // Required parameter
    private final String collectionName;

    // Optional parameter - initialized to default value
    private double threshold = 0.2;

    /** @param collectionName collection to compact */
    public Builder(@Nonnull String collectionName) {
      this.collectionName = collectionName;
    }

    /**
     * Optional. Default to 0.2. Segment will compact if and only if the percentage of entities
     * deleted exceeds the threshold.
     *
     * @param threshold The threshold
     * @return <code>Builder</code>
     */
    public Builder withThreshold(double threshold) {
      this.threshold = threshold;
      return this;
    }

    public CompactParam build() {
      return new CompactParam(this);
    }
  }
}
