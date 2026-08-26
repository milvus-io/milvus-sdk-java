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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for the {@code runAnalyzer} API.
 */
public class RunAnalyzerReq {
    private List<String> texts;
    private Map<String, Object> analyzerParams;
    private Boolean withDetail;
    private Boolean withHash;
    private String databaseName;
    private String collectionName;
    private String fieldName;
    private List<String> analyzerNames;

    private RunAnalyzerReq(RunAnalyzerReqBuilder builder) {
        this.texts = builder.texts;
        this.analyzerParams = builder.analyzerParams;
        this.withDetail = builder.withDetail;
        this.withHash = builder.withHash;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.analyzerNames = builder.analyzerNames;
    }

    /**
     * Creates a new {@code RunAnalyzerReq} builder.
     *
     * @return the builder
     */
    public static RunAnalyzerReqBuilder builder() {
        return new RunAnalyzerReqBuilder();
    }

    /**
     * Returns the texts to analyze.
     *
     * @return the texts
     */
    public List<String> getTexts() {
        return texts;
    }

    /**
     * Sets the texts to analyze.
     *
     * @param texts the texts
     */
    public void setTexts(List<String> texts) {
        this.texts = texts;
    }

    /**
     * Returns the analyzer configuration parameters.
     *
     * @return the analyzer parameters
     */
    public Map<String, Object> getAnalyzerParams() {
        return analyzerParams;
    }

    /**
     * Sets the analyzer configuration parameters.
     *
     * @param analyzerParams the analyzer parameters
     */
    public void setAnalyzerParams(Map<String, Object> analyzerParams) {
        this.analyzerParams = analyzerParams;
    }

    /**
     * Returns whether detailed token information is requested.
     *
     * @return {@code true} if detailed token information is requested
     */
    public Boolean getWithDetail() {
        return withDetail;
    }

    /**
     * Sets whether detailed token information is requested.
     *
     * @param withDetail {@code true} if detailed token information is requested
     */
    public void setWithDetail(Boolean withDetail) {
        this.withDetail = withDetail;
    }

    /**
     * Returns whether token hash values are requested.
     *
     * @return {@code true} if token hash values are requested
     */
    public Boolean getWithHash() {
        return withHash;
    }

    /**
     * Sets whether token hash values are requested.
     *
     * @param withHash {@code true} if token hash values are requested
     */
    public void setWithHash(Boolean withHash) {
        this.withHash = withHash;
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
     * Returns the field name to analyze.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the field name to analyze.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the names of the analyzers to run.
     *
     * @return the analyzer names
     */
    public List<String> getAnalyzerNames() {
        return analyzerNames;
    }

    /**
     * Sets the names of the analyzers to run.
     *
     * @param analyzerNames the analyzer names
     */
    public void setAnalyzerNames(List<String> analyzerNames) {
        this.analyzerNames = analyzerNames;
    }

    @Override
    public String toString() {
        return "RunAnalyzerReq{" +
                "texts=" + texts +
                ", analyzerParams=" + analyzerParams +
                ", withDetail=" + withDetail +
                ", withHash=" + withHash +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", analyzerNames=" + analyzerNames +
                '}';
    }

    public static class RunAnalyzerReqBuilder {
        private List<String> texts = new ArrayList<>();
        private Map<String, Object> analyzerParams = new HashMap<>();
        private Boolean withDetail = Boolean.FALSE;
        private Boolean withHash = Boolean.FALSE;
        private String databaseName = "";
        private String collectionName = "";
        private String fieldName = "";
        private List<String> analyzerNames = new ArrayList<>();

        /**
         * Sets the texts to analyze.
         *
         * @param texts the texts
         * @return this builder
         */
        public RunAnalyzerReqBuilder texts(List<String> texts) {
            this.texts = texts;
            return this;
        }

        /**
         * Sets the analyzer configuration parameters.
         *
         * @param analyzerParams the analyzer parameters
         * @return this builder
         */
        public RunAnalyzerReqBuilder analyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
            return this;
        }

        /**
         * Sets whether detailed token information is requested.
         *
         * @param withDetail {@code true} if detailed token information is requested
         * @return this builder
         */
        public RunAnalyzerReqBuilder withDetail(Boolean withDetail) {
            this.withDetail = withDetail;
            return this;
        }

        /**
         * Sets whether token hash values are requested.
         *
         * @param withHash {@code true} if token hash values are requested
         * @return this builder
         */
        public RunAnalyzerReqBuilder withHash(Boolean withHash) {
            this.withHash = withHash;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public RunAnalyzerReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public RunAnalyzerReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the field name to analyze.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public RunAnalyzerReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the names of the analyzers to run.
         *
         * @param analyzerNames the analyzer names
         * @return this builder
         */
        public RunAnalyzerReqBuilder analyzerNames(List<String> analyzerNames) {
            this.analyzerNames = analyzerNames;
            return this;
        }

        /**
         * Builds the {@link RunAnalyzerReq}.
         *
         * @return the request
         */
        public RunAnalyzerReq build() {
            return new RunAnalyzerReq(this);
        }
    }
}
