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

    public static RunAnalyzerReqBuilder builder() {
        return new RunAnalyzerReqBuilder();
    }

    public List<String> getTexts() {
        return texts;
    }

    public void setTexts(List<String> texts) {
        this.texts = texts;
    }

    public Map<String, Object> getAnalyzerParams() {
        return analyzerParams;
    }

    public void setAnalyzerParams(Map<String, Object> analyzerParams) {
        this.analyzerParams = analyzerParams;
    }

    public Boolean getWithDetail() {
        return withDetail;
    }

    public void setWithDetail(Boolean withDetail) {
        this.withDetail = withDetail;
    }

    public Boolean getWithHash() {
        return withHash;
    }

    public void setWithHash(Boolean withHash) {
        this.withHash = withHash;
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

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> getAnalyzerNames() {
        return analyzerNames;
    }

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

        public RunAnalyzerReqBuilder texts(List<String> texts) {
            this.texts = texts;
            return this;
        }

        public RunAnalyzerReqBuilder analyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
            return this;
        }

        public RunAnalyzerReqBuilder withDetail(Boolean withDetail) {
            this.withDetail = withDetail;
            return this;
        }

        public RunAnalyzerReqBuilder withHash(Boolean withHash) {
            this.withHash = withHash;
            return this;
        }

        public RunAnalyzerReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public RunAnalyzerReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RunAnalyzerReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public RunAnalyzerReqBuilder analyzerNames(List<String> analyzerNames) {
            this.analyzerNames = analyzerNames;
            return this;
        }

        public RunAnalyzerReq build() {
            return new RunAnalyzerReq(this);
        }
    }
}
