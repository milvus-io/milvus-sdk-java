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
 * Contains the returned <code>response</code> and <code>collectionMapping</code> for <code>describeCollection
 * </code>
 */
public class DescribeCollectionResponse {
  private final Response response;
  private final CollectionMapping collectionMapping;

  DescribeCollectionResponse(Response response, @Nullable CollectionMapping collectionMapping) {
    this.response = response;
    this.collectionMapping = collectionMapping;
  }

  /**
   * @return an <code>Optional</code> object which may or may not contain a <code>CollectionMapping</code>
   *     object
   * @see Optional
   */
  public Optional<CollectionMapping> getCollectionMapping() {
    return Optional.ofNullable(collectionMapping);
  }

  public Response getResponse() {
    return response;
  }

  public boolean ok() {
    return response.ok();
  }

  @Override
  public String toString() {
    return String.format(
        "DescribeCollectionResponse {%s, %s}",
        response.toString(), collectionMapping == null ? "Collection mapping = None" : collectionMapping.toString());
  }
}
