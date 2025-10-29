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

public class DeleteResp {
    private long deleteCnt;

    private DeleteResp(DeleteRespBuilder builder) {
        this.deleteCnt = builder.deleteCnt;
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

    @Override
    public String toString() {
        return "DeleteResp{" +
                "deleteCnt=" + deleteCnt +
                '}';
    }

    public static class DeleteRespBuilder {
        private long deleteCnt;

        public DeleteRespBuilder deleteCnt(long deleteCnt) {
            this.deleteCnt = deleteCnt;
            return this;
        }

        public DeleteResp build() {
            return new DeleteResp(this);
        }
    }
}
