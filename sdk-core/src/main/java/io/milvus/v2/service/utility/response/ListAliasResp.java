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

package io.milvus.v2.service.utility.response;

import java.util.List;

public class ListAliasResp {
    private String collectionName;
    private List<String> alias;

    private ListAliasResp(ListAliasRespBuilder builder) {
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
    }

    public static ListAliasRespBuilder builder() {
        return new ListAliasRespBuilder();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> getAlias() {
        return alias;
    }

    public void setAlias(List<String> alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "ListAliasResp{" +
                "collectionName='" + collectionName + '\'' +
                ", alias=" + alias +
                '}';
    }

    public static class ListAliasRespBuilder {
        private String collectionName;
        private List<String> alias;

        public ListAliasRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public ListAliasRespBuilder alias(List<String> alias) {
            this.alias = alias;
            return this;
        }

        public ListAliasResp build() {
            return new ListAliasResp(this);
        }
    }
}
