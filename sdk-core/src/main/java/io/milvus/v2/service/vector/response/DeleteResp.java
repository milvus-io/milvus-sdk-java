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

/**
 * Response returned by the {@code delete} API.
 */
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

    /**
     * Creates a new {@code DeleteResp} builder.
     *
     * @return the builder
     */
    public static DeleteRespBuilder builder() {
        return new DeleteRespBuilder();
    }

    /**
     * Returns the number of entities deleted.
     *
     * @return the delete count
     */
    public long getDeleteCnt() {
        return deleteCnt;
    }

    /**
     * Sets the number of entities deleted.
     *
     * @param deleteCnt the delete count
     */
    public void setDeleteCnt(long deleteCnt) {
        this.deleteCnt = deleteCnt;
    }

    /**
     * Returns the primary keys of the deleted entities.
     *
     * @return the deleted primary keys
     */
    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * Sets the primary keys of the deleted entities.
     *
     * @param primaryKeys the deleted primary keys
     */
    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    /**
     * Returns the time cost of the delete operation.
     *
     * @return the cost
     */
    public Long getCost() {
        return cost;
    }

    /**
     * Sets the time cost of the delete operation.
     *
     * @param cost the cost
     */
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

        /**
         * Sets the number of entities deleted.
         *
         * @param deleteCnt the delete count
         * @return this builder
         */
        public DeleteRespBuilder deleteCnt(long deleteCnt) {
            this.deleteCnt = deleteCnt;
            return this;
        }

        /**
         * Sets the primary keys of the deleted entities.
         *
         * @param primaryKeys the deleted primary keys
         * @return this builder
         */
        public DeleteRespBuilder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        /**
         * Sets the time cost of the delete operation.
         *
         * @param cost the cost
         * @return this builder
         */
        public DeleteRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        /**
         * Builds the {@link DeleteResp}.
         *
         * @return the response
         */
        public DeleteResp build() {
            return new DeleteResp(this);
        }
    }
}
