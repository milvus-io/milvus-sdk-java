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

package io.milvus.v2.service.vector.request;

import java.util.List;

public class GetReq {
    private String databaseName;
    private String collectionName;
    private String partitionName = "";
    private List<Object> ids;
    private List<String> outputFields;

    private GetReq(GetReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.ids = builder.ids;
        this.outputFields = builder.outputFields;
    }

    public static GetReqBuilder builder() {
        return new GetReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public List<Object> getIds() {
        return ids;
    }

    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public String toString() {
        return "GetReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", ids=" + ids +
                ", outputFields=" + outputFields +
                '}';
    }

    public static class GetReqBuilder {
        private String databaseName;
        private String collectionName;
        private String partitionName = "";
        private List<Object> ids;
        private List<String> outputFields;

        public GetReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public GetReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public GetReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        public GetReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public GetReq build() {
            return new GetReq(this);
        }
    }
}
