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

import io.milvus.exception.ParamException;
import io.milvus.response.QueryResultsWrapper;

import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */


public class SearchResponse {
    private List<List<QueryResultsWrapper.RowRecord>> rowRecords;

    private SearchResponse(Builder builder) {
        this.rowRecords = builder.rowRecords;
    }

    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the rowRecords.
     *
     * @return the rowRecords
     */


    public List<List<QueryResultsWrapper.RowRecord>> getRowRecords() {
        return rowRecords;
    }

    /**
     * Sets the rowRecords.
     *
     * @param rowRecords the rowRecords
     */


    public void setRowRecords(List<List<QueryResultsWrapper.RowRecord>> rowRecords) {
        this.rowRecords = rowRecords;
    }

    /**
     * In old versions(less or equal than v2.3.2), this method only returns results of the first target vector
     * Mark is as deprecated, keep it to compatible with the legacy code
     *
     * @return List of <code>QueryResultsWrapper.RowRecord</code>
     */
    @Deprecated
    public List<QueryResultsWrapper.RowRecord> getRowRecordsForFirstTarget() {
        return getRowRecords(0);
    }

    /**
     * Gets the row records of the search result at the specified target index.
     *
     * @param indexOfTarget the index of the target search
     * @return the row records of the target search
     */


    public List<QueryResultsWrapper.RowRecord> getRowRecords(int indexOfTarget) {
        if (indexOfTarget >= rowRecords.size()) {
            throw new ParamException("The indexOfTarget value " + indexOfTarget
                    + " exceeds results count " + rowRecords.size());
        }

        return rowRecords.get(indexOfTarget);
    }

    @Override
    public String toString() {
        return "SearchResponse{" +
                "rowRecords=" + rowRecords +
                '}';
    }

    /**
     * Builder for {@link SearchResponse} class.
     */


    public static class Builder {
        private List<List<QueryResultsWrapper.RowRecord>> rowRecords;

        /**
         * Sets the rowRecords.
         *
         * @param rowRecords the rowRecords
         * @return this builder
         */


        public Builder rowRecords(List<List<QueryResultsWrapper.RowRecord>> rowRecords) {
            this.rowRecords = rowRecords;
            return this;
        }

        /**
         * Builds the SearchResponse.
         *
         * @return the built SearchResponse
         */


        public SearchResponse build() {
            return new SearchResponse(this);
        }
    }
}
