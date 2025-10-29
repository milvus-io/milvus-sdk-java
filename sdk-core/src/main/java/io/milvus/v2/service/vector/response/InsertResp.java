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

public class InsertResp {
    // TODO: the first character should be lower case, add a new member and deprecate the old member
    private long InsertCnt;
    private List<Object> primaryKeys;

    private InsertResp(InsertRespBuilder builder) {
        this.InsertCnt = builder.InsertCnt;
        this.primaryKeys = builder.primaryKeys;
    }

    public static InsertRespBuilder builder() {
        return new InsertRespBuilder();
    }

    public long getInsertCnt() {
        return InsertCnt;
    }

    public void setInsertCnt(long insertCnt) {
        InsertCnt = insertCnt;
    }

    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public String toString() {
        return "InsertResp{" +
                "InsertCnt=" + InsertCnt +
                ", primaryKeys=" + primaryKeys +
                '}';
    }

    public static class InsertRespBuilder {
        private long InsertCnt;
        private List<Object> primaryKeys = new ArrayList<>();

        public InsertRespBuilder InsertCnt(long insertCnt) {
            InsertCnt = insertCnt;
            return this;
        }

        public InsertRespBuilder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public InsertResp build() {
            return new InsertResp(this);
        }
    }
}
