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

import io.milvus.v2.common.ConsistencyLevel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for the {@code delete} API.
 */
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
    private ConsistencyLevel consistencyLevel;

    private DeleteReq(DeleteReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.filter = builder.filter;
        this.ids = builder.ids;
        this.filterTemplateValues = builder.filterTemplateValues;
        this.consistencyLevel = builder.consistencyLevel;
    }

    /**
     * Creates a new {@code DeleteReq} builder.
     *
     * @return the builder
     */
    public static DeleteReqBuilder builder() {
        return new DeleteReqBuilder();
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the partition name to delete from.
     *
     * @return the partition name
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Sets the partition name to delete from.
     *
     * @param partitionName the partition name
     */
    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    /**
     * Returns the filter expression that selects the entities to delete.
     *
     * @return the filter expression
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Sets the filter expression that selects the entities to delete.
     *
     * @param filter the filter expression
     */
    public void setFilter(String filter) {
        this.filter = filter;
    }

    /**
     * Returns the primary key values of the entities to delete.
     *
     * @return the primary key values
     */
    public List<Object> getIds() {
        return ids;
    }

    /**
     * Sets the primary key values of the entities to delete.
     *
     * @param ids the primary key values
     */
    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    /**
     * Returns the expression template values used to improve expression parsing performance.
     *
     * @return the filter template values
     */
    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    /**
     * Sets the expression template values used to improve expression parsing performance.
     *
     * @param filterTemplateValues the filter template values
     */
    public void setFilterTemplateValues(Map<String, Object> filterTemplateValues) {
        this.filterTemplateValues = filterTemplateValues;
    }

    /**
     * Returns the consistency level for the delete operation.
     *
     * @return the consistency level, or {@code null} for the server default
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level for the delete operation.
     *
     * @param consistencyLevel the consistency level, or {@code null} for the server default
     */
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public String toString() {
        return "DeleteReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", filter='" + filter + '\'' +
                ", ids=" + ids +
                ", consistencyLevel=" + consistencyLevel +
//                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static class DeleteReqBuilder {
        private String databaseName = "";
        private String collectionName;
        private String partitionName = "";
        private String filter;
        private List<Object> ids;
        private Map<String, Object> filterTemplateValues = new HashMap<>();
        private ConsistencyLevel consistencyLevel;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DeleteReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DeleteReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition name to delete from.
         *
         * @param partitionName the partition name
         * @return this builder
         */
        public DeleteReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the filter expression that selects the entities to delete.
         *
         * @param filter the filter expression
         * @return this builder
         */
        public DeleteReqBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets the primary key values of the entities to delete.
         *
         * @param ids the primary key values
         * @return this builder
         */
        public DeleteReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        /**
         * Sets the expression template values used to improve expression parsing performance.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */
        public DeleteReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Sets the consistency level for the delete operation.
         *
         * @param consistencyLevel the consistency level, or {@code null} for the server default
         * @return this builder
         */
        public DeleteReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Builds the {@link DeleteReq}.
         *
         * @return the request
         */
        public DeleteReq build() {
            return new DeleteReq(this);
        }
    }
}
