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
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
@Builder
public class SearchResponse {
    public List<List<QueryResultsWrapper.RowRecord>> rowRecords;

    /**
     * In old versions(less or equal than v2.3.2), this method only returns results of the first target vector
     * Mark is as deprecated, keep it to compatible with the legacy code
     *
     * @return List of <code>QueryResultsWrapper.RowRecord</code>
     */
    @Deprecated
    public List<QueryResultsWrapper.RowRecord> getRowRecords() {
        return getRowRecords(0);
    }

    public List<QueryResultsWrapper.RowRecord> getRowRecords(int indexOfTarget) {
        if (indexOfTarget >= rowRecords.size()) {
            throw new ParamException("The indexOfTarget value " + indexOfTarget
                    + " exceeds results count " + rowRecords.size());
        }

        return rowRecords.get(indexOfTarget);
    }
}
