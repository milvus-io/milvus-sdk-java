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

import java.util.ArrayList;
import java.util.List;

public class DropDatabasePropertiesReq {
    private String databaseName;
    private List<String> propertyKeys;

    private DropDatabasePropertiesReq(DropDatabasePropertiesReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.propertyKeys = builder.propertyKeys;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropDatabasePropertiesReq{" +
                "databaseName='" + databaseName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    public static DropDatabasePropertiesReqBuilder builder() {
        return new DropDatabasePropertiesReqBuilder();
    }

    public static class DropDatabasePropertiesReqBuilder {
        private String databaseName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropDatabasePropertiesReqBuilder() {
        }

        public DropDatabasePropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DropDatabasePropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        public DropDatabasePropertiesReq build() {
            return new DropDatabasePropertiesReq(this);
        }
    }
}
