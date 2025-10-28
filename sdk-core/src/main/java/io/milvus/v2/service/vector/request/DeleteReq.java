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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteReq {
    private String databaseName;
    private String collectionName;
    private String partitionName;
    private String filter;
    private List<Object> ids;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private DeleteReq(DeleteReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.filter = builder.filter;
        this.ids = builder.ids;
        this.filterTemplateValues = builder.filterTemplateValues;
    }

    public static DeleteReqBuilder builder() {
        return new DeleteReqBuilder();
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

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<Object> getIds() {
        return ids;
    }

    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    public void setFilterTemplateValues(Map<String, Object> filterTemplateValues) {
        this.filterTemplateValues = filterTemplateValues;
    }

    @Override
    public String toString() {
        return "DeleteReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", filter='" + filter + '\'' +
                ", ids=" + ids +
                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static class DeleteReqBuilder {
        private String databaseName = "";
        private String collectionName;
        private String partitionName = "";
        private String filter;
        private List<Object> ids;
        private Map<String, Object> filterTemplateValues = new HashMap<>();

        public DeleteReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DeleteReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DeleteReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public DeleteReqBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public DeleteReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        public DeleteReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        public DeleteReq build() {
            return new DeleteReq(this);
        }
    }
}
