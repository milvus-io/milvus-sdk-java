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

package io.milvus.v2.service.vector.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class GetResp {
    private List<QueryResp.QueryResult> getResults;

    private GetResp(Builder builder) {
        this.getResults = builder.getResults;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<QueryResp.QueryResult> getGetResults() {
        return getResults;
    }

    public void setGetResults(List<QueryResp.QueryResult> getResults) {
        this.getResults = getResults;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetResp that = (GetResp) obj;
        return new EqualsBuilder()
                .append(getResults, that.getResults)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getResults)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "GetResp{" +
                "getResults=" + getResults +
                '}';
    }

    public static class Builder {
        private List<QueryResp.QueryResult> getResults;

        public Builder getResults(List<QueryResp.QueryResult> getResults) {
            this.getResults = getResults;
            return this;
        }

        public GetResp build() {
            return new GetResp(this);
        }
    }
}
