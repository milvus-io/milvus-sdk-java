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

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Contains the returned <code>response</code> and <code>index</code> for <code>describeIndex</code>
 */
public class DescribeIndexResponse {
  private final Response response;
  private final Index index;

  public DescribeIndexResponse(Response response, @Nullable Index index) {
    this.response = response;
    this.index = index;
  }

  /**
   * @return an <code>Optional</code> object which may or may not contain an <code>Index</code>
   *     object
   * @see Optional
   */
  public Optional<Index> getIndex() {
    return Optional.ofNullable(index);
  }

  public Response getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return String.format(
        "DescribeIndexResponse {%s, %s}",
        response.toString(), index == null ? "Index = Null" : index.toString());
  }
}
