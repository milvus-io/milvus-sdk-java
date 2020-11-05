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

/** Represents available data types. */
public enum DataType {
  NONE(0),
  BOOL(1),
  INT8(2),
  INT16(3),
  INT32(4),
  INT64(5),

  FLOAT(10),
  DOUBLE(11),

  STRING(20),

  VECTOR_BINARY(100),
  VECTOR_FLOAT(101),

  UNKNOWN(-1);

  private final int val;

  DataType(int val) {
    this.val = val;
  }

  public static DataType valueOf(int val) {
    Optional<DataType> search =
        Arrays.stream(values()).filter(dataType -> dataType.val == val).findFirst();
    return search.orElse(UNKNOWN);
  }

  public int getVal() {
    return val;
  }
}
