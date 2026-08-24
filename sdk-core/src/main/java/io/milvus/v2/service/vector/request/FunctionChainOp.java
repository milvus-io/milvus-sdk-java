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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A single operation in a {@link FunctionChain} pipeline. Mirrors PyMilvus's
 * {@code FunctionChainOp}. The {@code op} name is a server-recognized operation name; the
 * Milvus proto lists e.g. {@code "map"}, {@code "filter"}, {@code "sort"}, {@code "limit"},
 * and {@code "merge"}. The static {@code map}/{@code sort}/{@code limit} factories set a
 * valid op; the raw builder only requires a non-empty string so future server ops remain
 * expressible.
 */
public class FunctionChainOp {
    public static final String OP_MAP = "map";
    public static final String OP_SORT = "sort";
    public static final String OP_LIMIT = "limit";

    private final String op;
    private final FunctionChainExpr expr;
    private final List<String> inputs;
    private final List<String> outputs;
    private final Map<String, FunctionParamValue> params;

    private FunctionChainOp(FunctionChainOpBuilder builder) {
        this.op = builder.op;
        this.expr = builder.expr;
        this.inputs = new ArrayList<>(builder.inputs);
        this.outputs = new ArrayList<>(builder.outputs);
        this.params = new LinkedHashMap<>(builder.params);
    }

    public static FunctionChainOpBuilder builder() {
        return new FunctionChainOpBuilder();
    }

    public String getOp() {
        return op;
    }

    public FunctionChainExpr getExpr() {
        return expr;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    public Map<String, FunctionParamValue> getParams() {
        return params;
    }

    public io.milvus.grpc.FunctionChainOp toGrpc() {
        io.milvus.grpc.FunctionChainOp.Builder builder = io.milvus.grpc.FunctionChainOp.newBuilder().setOp(op);
        if (expr != null) {
            builder.setExpr(expr.toGrpc());
        }
        builder.addAllInputs(inputs);
        builder.addAllOutputs(outputs);
        params.forEach((k, v) -> builder.putParams(k, v.toGrpc()));
        return builder.build();
    }

    static FunctionChainOp map(String output, FunctionChainExpr expr) {
        if (output == null || output.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function chain map output must be a non-empty string");
        }
        if (expr == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function chain map expr must not be null");
        }
        return builder().op(OP_MAP).expr(expr).outputs(Collections.singletonList(output)).build();
    }

    static FunctionChainOp sort(String by, boolean desc, String tieBreakCol) {
        if (by == null || by.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function chain sort by must be a non-empty string");
        }
        List<String> inputs = new ArrayList<>();
        inputs.add(by);
        Map<String, FunctionParamValue> params = new LinkedHashMap<>();
        params.put("column", FunctionParamValue.of(by));
        params.put("desc", FunctionParamValue.of(desc));
        if (tieBreakCol != null && !tieBreakCol.isEmpty()) {
            params.put("tie_break_col", FunctionParamValue.of(tieBreakCol));
            inputs.add(tieBreakCol);
        }
        return builder().op(OP_SORT).inputs(inputs).params(params).build();
    }

    static FunctionChainOp limit(int limit, int offset) {
        if (limit <= 0) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function chain limit must be a positive integer");
        }
        if (offset < 0) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function chain offset must be a non-negative integer");
        }
        Map<String, FunctionParamValue> params = new LinkedHashMap<>();
        params.put("limit", FunctionParamValue.of((long) limit));
        params.put("offset", FunctionParamValue.of((long) offset));
        return builder().op(OP_LIMIT).params(params).build();
    }

    public static class FunctionChainOpBuilder {
        private String op;
        private FunctionChainExpr expr;
        private final List<String> inputs = new ArrayList<>();
        private final List<String> outputs = new ArrayList<>();
        private final Map<String, FunctionParamValue> params = new LinkedHashMap<>();

        private FunctionChainOpBuilder() {
        }

        public FunctionChainOpBuilder op(String op) {
            this.op = op;
            return this;
        }

        public FunctionChainOpBuilder expr(FunctionChainExpr expr) {
            this.expr = expr;
            return this;
        }

        public FunctionChainOpBuilder inputs(List<String> inputs) {
            if (inputs == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain op inputs must not be null");
            }
            this.inputs.clear();
            this.inputs.addAll(inputs);
            return this;
        }

        public FunctionChainOpBuilder outputs(List<String> outputs) {
            if (outputs == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain op outputs must not be null");
            }
            this.outputs.clear();
            this.outputs.addAll(outputs);
            return this;
        }

        public FunctionChainOpBuilder params(Map<String, FunctionParamValue> params) {
            if (params == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain op params must not be null");
            }
            this.params.clear();
            this.params.putAll(params);
            return this;
        }

        public FunctionChainOp build() {
            if (op == null || op.isEmpty()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain op name must be a non-empty string");
            }
            return new FunctionChainOp(this);
        }
    }
}
