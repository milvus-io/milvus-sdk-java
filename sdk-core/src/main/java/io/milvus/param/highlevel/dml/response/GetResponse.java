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

package io.milvus.param.highlevel.dml.response;

import io.milvus.response.QueryResultsWrapper;

import java.util.List;

/**
 * Parameters for <code>get</code> interface.
 */
public class GetResponse {
    public List<QueryResultsWrapper.RowRecord> rowRecords;

    private GetResponse(Builder builder) {
        this.rowRecords = builder.rowRecords;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getter method to replace @Getter annotation
    public List<QueryResultsWrapper.RowRecord> getRowRecords() {
        return rowRecords;
    }

    /**
     * Builder for {@link GetResponse} class to replace @Builder annotation.
     */
    public static class Builder {
        private List<QueryResultsWrapper.RowRecord> rowRecords;

        private Builder() {
        }

        public Builder rowRecords(List<QueryResultsWrapper.RowRecord> rowRecords) {
            this.rowRecords = rowRecords;
            return this;
        }

        public GetResponse build() {
            return new GetResponse(this);
        }
    }
}
