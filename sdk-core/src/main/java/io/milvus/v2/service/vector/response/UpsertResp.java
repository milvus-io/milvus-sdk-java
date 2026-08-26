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
 * Response returned by the {@code upsert} API.
 */
public class UpsertResp {
    private long upsertCnt;

    // From v2.4.10, milvus allows upsert for auto-id=true, the server will return a new pk.
    // the new pk is not equal to the original pk, the original entity is deleted, and a new entity
    // is created with this new pk. Here we return this new pk to user.
    private List<Object> primaryKeys;
    private Long cost;

    private UpsertResp(UpsertRespBuilder builder) {
        this.upsertCnt = builder.upsertCnt;
        this.primaryKeys = builder.primaryKeys;
        this.cost = builder.cost;
    }

    // Getters and Setters
    /**
     * Returns the number of entities upserted.
     *
     * @return the upsert count
     */
    public long getUpsertCnt() {
        return upsertCnt;
    }

    /**
     * Sets the number of entities upserted.
     *
     * @param upsertCnt the upsert count
     */
    public void setUpsertCnt(long upsertCnt) {
        this.upsertCnt = upsertCnt;
    }

    /**
     * Returns the primary keys of the upserted entities.
     *
     * @return the upserted primary keys
     */
    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * Sets the primary keys of the upserted entities.
     *
     * @param primaryKeys the upserted primary keys
     */
    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    /**
     * Returns the time cost of the upsert operation.
     *
     * @return the cost
     */
    public Long getCost() {
        return cost;
    }

    /**
     * Sets the time cost of the upsert operation.
     *
     * @param cost the cost
     */
    public void setCost(Long cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "UpsertResp{" +
                "upsertCnt=" + upsertCnt +
                ", primaryKeys=" + primaryKeys +
                ", cost=" + cost +
                '}';
    }

    /**
     * Creates a new {@code UpsertResp} builder.
     *
     * @return the builder
     */
    public static UpsertRespBuilder builder() {
        return new UpsertRespBuilder();
    }

    public static class UpsertRespBuilder {
        private long upsertCnt;
        private List<Object> primaryKeys = new ArrayList<>(); // default value
        private Long cost;

        private UpsertRespBuilder() {
        }

        /**
         * Sets the number of entities upserted.
         *
         * @param upsertCnt the upsert count
         * @return this builder
         */
        public UpsertRespBuilder upsertCnt(long upsertCnt) {
            this.upsertCnt = upsertCnt;
            return this;
        }

        /**
         * Sets the primary keys of the upserted entities.
         *
         * @param primaryKeys the upserted primary keys
         * @return this builder
         */
        public UpsertRespBuilder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        /**
         * Sets the time cost of the upsert operation.
         *
         * @param cost the cost
         * @return this builder
         */
        public UpsertRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        /**
         * Builds the {@link UpsertResp}.
         *
         * @return the response
         */
        public UpsertResp build() {
            return new UpsertResp(this);
        }
    }
}
