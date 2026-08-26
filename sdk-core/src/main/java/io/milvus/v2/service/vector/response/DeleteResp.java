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

import java.util.ArrayList;
import java.util.List;

public class DeleteResp {
    private long deleteCnt;
    /**
     * Primary keys of the deleted entities. Note: Milvus servers >= 2.3.2 no longer echo the
     * deleted primary keys in the delete response, so this list will be empty against a modern
     * server. It is only populated when talking to an older server that returns the keys.
     */
    private List<Object> primaryKeys;
    private Long cost;

    private DeleteResp(DeleteRespBuilder builder) {
        this.deleteCnt = builder.deleteCnt;
        this.primaryKeys = builder.primaryKeys;
        this.cost = builder.cost;
    }

    public static DeleteRespBuilder builder() {
        return new DeleteRespBuilder();
    }

    public long getDeleteCnt() {
        return deleteCnt;
    }

    public void setDeleteCnt(long deleteCnt) {
        this.deleteCnt = deleteCnt;
    }

    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "DeleteResp{" +
                "deleteCnt=" + deleteCnt +
                ", primaryKeys=" + primaryKeys +
                ", cost=" + cost +
                '}';
    }

    public static class DeleteRespBuilder {
        private long deleteCnt;
        private List<Object> primaryKeys = new ArrayList<>();
        private Long cost;

        public DeleteRespBuilder deleteCnt(long deleteCnt) {
            this.deleteCnt = deleteCnt;
            return this;
        }

        public DeleteRespBuilder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public DeleteRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        public DeleteResp build() {
            return new DeleteResp(this);
        }
    }
}
