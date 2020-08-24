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

/**
 * Contains the returned <code>response</code> and <code>hasCollection</code> for <code>
 * hasCollection</code>
 */
public class HasCollectionResponse {
  private final Response response;
  private final boolean hasCollection;

  HasCollectionResponse(Response response, boolean hasCollection) {
    this.response = response;
    this.hasCollection = hasCollection;
  }

  /** @return <code>true</code> if the collection is present */
  public boolean hasCollection() {
    return hasCollection;
  }

  public Response getResponse() {
    return response;
  }

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }

  @Override
  public String toString() {
    return String.format(
        "HasCollectionResponse {%s, has collection = %s}", response.toString(), hasCollection);
  }
}
