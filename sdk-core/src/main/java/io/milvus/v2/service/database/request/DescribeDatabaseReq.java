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

package io.milvus.v2.service.database.request;

public class DescribeDatabaseReq {
    private String databaseName;

    private DescribeDatabaseReq(DescribeDatabaseReqBuilder builder) {
        this.databaseName = builder.databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String toString() {
        return "DescribeDatabaseReq{" +
                "databaseName='" + databaseName + '\'' +
                '}';
    }

    public static DescribeDatabaseReqBuilder builder() {
        return new DescribeDatabaseReqBuilder();
    }

    public static class DescribeDatabaseReqBuilder {
        private String databaseName;

        private DescribeDatabaseReqBuilder() {
        }

        public DescribeDatabaseReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DescribeDatabaseReq build() {
            return new DescribeDatabaseReq(this);
        }
    }
}
