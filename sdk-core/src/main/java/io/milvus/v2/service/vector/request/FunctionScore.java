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

import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionScore {
    private List<CreateCollectionReq.Function> functions;
    private Map<String, String> params;

    // Private constructor for builder
    private FunctionScore(FunctionScoreBuilder builder) {
        this.functions = builder.functions != null ? builder.functions : new ArrayList<>();
        this.params = builder.params != null ? builder.params : new HashMap<>();
    }

    // Static method to create builder
    public static FunctionScoreBuilder builder() {
        return new FunctionScoreBuilder();
    }

    // Getter methods
    public List<CreateCollectionReq.Function> getFunctions() {
        return functions;
    }

    public Map<String, String> getParams() {
        return params;
    }

    // Setter methods
    public void setFunctions(List<CreateCollectionReq.Function> functions) {
        this.functions = functions;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return "FunctionScore{" +
                "functions=" + functions +
                ", params=" + params +
                '}';
    }

    // Builder class
    public static class FunctionScoreBuilder {
        private List<CreateCollectionReq.Function> functions;
        private Map<String, String> params;

        public FunctionScoreBuilder() {
            this.functions = new ArrayList<>();
            this.params = new HashMap<>();
        }

        public FunctionScoreBuilder functions(List<CreateCollectionReq.Function> functions) {
            this.functions = functions;
            return this;
        }

        public FunctionScoreBuilder params(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public FunctionScoreBuilder addFunction(CreateCollectionReq.Function func) {
            if (this.functions == null) {
                this.functions = new ArrayList<>();
            }
            this.functions.add(func);
            return this;
        }

        public FunctionScore build() {
            return new FunctionScore(this);
        }
    }
}
