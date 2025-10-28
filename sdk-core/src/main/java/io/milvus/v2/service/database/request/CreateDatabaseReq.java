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

import java.util.HashMap;
import java.util.Map;

public class CreateDatabaseReq {
    private String databaseName;
    private Map<String, String> properties;

    private CreateDatabaseReq(CreateDatabaseReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.properties = builder.properties;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "CreateDatabaseReq{" +
                "databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static CreateDatabaseReqBuilder builder() {
        return new CreateDatabaseReqBuilder();
    }

    public static class CreateDatabaseReqBuilder {
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private CreateDatabaseReqBuilder() {
        }

        public CreateDatabaseReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CreateDatabaseReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public CreateDatabaseReq build() {
            return new CreateDatabaseReq(this);
        }
    }
}
