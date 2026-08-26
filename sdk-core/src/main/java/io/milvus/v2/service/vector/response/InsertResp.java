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
 * Response returned by the {@code insert} API.
 */
public class InsertResp {
    // TODO: the first character should be lower case, add a new member and deprecate the old member
    private long InsertCnt;
    private List<Object> primaryKeys;
    private Long cost;

    private InsertResp(InsertRespBuilder builder) {
        this.InsertCnt = builder.InsertCnt;
        this.primaryKeys = builder.primaryKeys;
        this.cost = builder.cost;
    }

    /**
     * Creates a new {@code InsertResp} builder.
     *
     * @return the builder
     */
    public static InsertRespBuilder builder() {
        return new InsertRespBuilder();
    }

    /**
     * Returns the number of entities inserted.
     *
     * @return the insert count
     */
    public long getInsertCnt() {
        return InsertCnt;
    }

    /**
     * Sets the number of entities inserted.
     *
     * @param insertCnt the insert count
     */
    public void setInsertCnt(long insertCnt) {
        InsertCnt = insertCnt;
    }

    /**
     * Returns the primary keys of the inserted entities.
     *
     * @return the inserted primary keys
     */
    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * Sets the primary keys of the inserted entities.
     *
     * @param primaryKeys the inserted primary keys
     */
    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    /**
     * Returns the time cost of the insert operation.
     *
     * @return the cost
     */
    public Long getCost() {
        return cost;
    }

    /**
     * Sets the time cost of the insert operation.
     *
     * @param cost the cost
     */
    public void setCost(Long cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "InsertResp{" +
                "InsertCnt=" + InsertCnt +
                ", primaryKeys=" + primaryKeys +
                ", cost=" + cost +
                '}';
    }

    public static class InsertRespBuilder {
        private long InsertCnt;
        private List<Object> primaryKeys = new ArrayList<>();
        private Long cost;

        /**
         * Sets the number of entities inserted.
         *
         * @param insertCnt the insert count
         * @return this builder
         */
        public InsertRespBuilder InsertCnt(long insertCnt) {
            InsertCnt = insertCnt;
            return this;
        }

        /**
         * Sets the primary keys of the inserted entities.
         *
         * @param primaryKeys the inserted primary keys
         * @return this builder
         */
        public InsertRespBuilder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        /**
         * Sets the time cost of the insert operation.
         *
         * @param cost the cost
         * @return this builder
         */
        public InsertRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        /**
         * Builds the {@link InsertResp}.
         *
         * @return the response
         */
        public InsertResp build() {
            return new InsertResp(this);
        }
    }
}
