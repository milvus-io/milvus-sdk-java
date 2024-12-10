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

package io.milvus.response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.ImportResponse;
import lombok.NonNull;

/**
 * Util class to wrap response of <code>bulkInsert</code> interface.
 */
public class BulkInsertResponseWrapper {
    private final ImportResponse response;

    public BulkInsertResponseWrapper(@NonNull ImportResponse response) {
        this.response = response;
    }

    /**
     * Gets ID of the bulk insert task.
     *
     * @return Long ID of the bulk insert task
     */
    public long getTaskID() {
        if (response.getTasksCount() == 0) {
            throw new IllegalResponseException("no task id returned from server");
        }
        return response.getTasks(0);
    }

    /**
     * Construct a <code>String</code> by {@link BulkInsertResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "bulk insert task state{" +
                ", taskId:" + getTaskID() +
                '}';
    }
}
