/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.List;

/** Contains parameters for <code>searchInFiles</code> */
public class SearchInFilesParam {
  private final List<String> fileIds;
  private final SearchParam searchParam;
  private final long timeout;

  private SearchInFilesParam(@Nonnull Builder builder) {
    this.fileIds = builder.fileIds;
    this.searchParam = builder.searchParam;
    this.timeout = builder.timeout;
  }

  public List<String> getFileIds() {
    return fileIds;
  }

  public SearchParam getSearchParam() {
    return searchParam;
  }

  public long getTimeout() {
    return timeout;
  }

  /** Builder for <code>SearchInFilesParam</code> */
  public static class Builder {
    // Required parameters
    private final List<String> fileIds;
    private final SearchParam searchParam;

    // Optional parameters - initialized to default values
    private long timeout = 86400;

    /**
     * @param fileIds a <code>List</code> of file ids to search from
     * @param searchParam a <code>searchParam</code> object
     */
    public Builder(List<String> fileIds, SearchParam searchParam) {
      this.fileIds = fileIds;
      this.searchParam = searchParam;
    }

    /**
     * Optional. Sets the deadline from when the client RPC is set to when the response is picked up
     * by the client. Default to 86400s (1 day).
     *
     * @param timeout in seconds
     * @return <code>Builder</code>
     */
    public Builder withTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public SearchInFilesParam build() {
      return new SearchInFilesParam(this);
    }
  }
}
