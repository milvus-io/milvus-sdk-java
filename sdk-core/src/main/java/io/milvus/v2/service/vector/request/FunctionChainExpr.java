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

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A named function invocation used by a function-chain {@code map} operation.
 *
 * <p>Arguments are added with {@code arg(FunctionChainArg.col(...))} or
 * {@code arg(FunctionChainArg.literal(...))}, and keyword parameters with
 * {@code param(String, Object)}. This mirrors PyMilvus's {@code FunctionChainExpr}.
 */
public class FunctionChainExpr {
    private final String name;
    private final List<FunctionChainArg> args;
    private final Map<String, FunctionParamValue> params;

    private FunctionChainExpr(FunctionChainExprBuilder builder) {
        this.name = builder.name;
        this.args = new ArrayList<>(builder.args);
        this.params = new LinkedHashMap<>(builder.params);
    }

    public static FunctionChainExprBuilder builder() {
        return new FunctionChainExprBuilder();
    }

    public String getName() {
        return name;
    }

    public List<FunctionChainArg> getArgs() {
        return args;
    }

    public Map<String, FunctionParamValue> getParams() {
        return params;
    }

    public io.milvus.grpc.FunctionChainExpr toGrpc() {
        io.milvus.grpc.FunctionChainExpr.Builder builder = io.milvus.grpc.FunctionChainExpr.newBuilder().setName(name);
        args.forEach(arg -> builder.addArgs(arg.toGrpc()));
        params.forEach((k, v) -> builder.putParams(k, v.toGrpc()));
        return builder.build();
    }

    public static class FunctionChainExprBuilder {
        private String name;
        private final List<FunctionChainArg> args = new ArrayList<>();
        private final Map<String, FunctionParamValue> params = new LinkedHashMap<>();

        private FunctionChainExprBuilder() {
        }

        public FunctionChainExprBuilder name(String name) {
            this.name = name;
            return this;
        }

        public FunctionChainExprBuilder arg(FunctionChainArg arg) {
            if (arg == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain expression arg must not be null");
            }
            this.args.add(arg);
            return this;
        }

        public FunctionChainExprBuilder param(String key, Object value) {
            if (key == null || key.isEmpty()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain expression parameter names must be non-empty strings");
            }
            this.params.put(key, FunctionParamValue.from(value));
            return this;
        }

        public FunctionChainExpr build() {
            if (name == null || name.isEmpty()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain expression name must be a non-empty string");
            }
            return new FunctionChainExpr(this);
        }
    }
}
