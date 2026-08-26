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
import java.util.List;

/**
 * An ordered rerank/refine plan applied to search results. Mirrors PyMilvus's
 * {@code FunctionChain}: a fluent builder that composes {@code map}, {@code sort}, and
 * {@code limit} operations and serializes to the gRPC {@code FunctionChain} message.
 *
 * <pre>{@code
 * FunctionChain chain = FunctionChain.builder()
 *         .stage(FunctionChainStage.L2_RERANK)
 *         .name("fresh_popular_rerank")
 *         .map("$score", FunctionChainExpr.builder()
 *                 .name("num_combine")
 *                 .arg(FunctionChainArg.col("$score"))
 *                 .arg(FunctionChainArg.col("freshness"))
 *                 .param("mode", "weighted")
 *                 .param("weights", Arrays.asList(0.7, 0.2, 0.1))
 *                 .build())
 *         .sort("$score", true, "$id")
 *         .limit(10)
 *         .build();
 * }</pre>
 */
public class FunctionChain {
    private final FunctionChainStage stage;
    private final String name;
    private final List<FunctionChainOp> ops;

    private FunctionChain(FunctionChainBuilder builder) {
        this.stage = builder.stage;
        this.name = builder.name;
        this.ops = new ArrayList<>(builder.ops);
    }

    /**
     * Creates a new builder for {@link FunctionChain}.
     *
     * @return a new builder
     */
    public static FunctionChainBuilder builder() {
        return new FunctionChainBuilder();
    }

    /**
     * Returns the execution stage where this function chain runs.
     *
     * @return the execution stage
     */
    public FunctionChainStage getStage() {
        return stage;
    }

    /**
     * Returns the name of this function chain.
     *
     * @return the chain name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the ordered list of operations of this function chain.
     *
     * @return the chain operations
     */
    public List<FunctionChainOp> getOps() {
        return ops;
    }

    /**
     * Converts this function chain into the gRPC {@code FunctionChain} message.
     *
     * @return the gRPC function chain
     */
    public io.milvus.grpc.FunctionChain toGrpc() {
        io.milvus.grpc.FunctionChain.Builder builder = io.milvus.grpc.FunctionChain.newBuilder()
                .setName(name)
                .setStage(stage.toGrpc());
        ops.forEach(op -> builder.addOps(op.toGrpc()));
        return builder.build();
    }

    /**
     * Builder for {@link FunctionChain}.
     */
    public static class FunctionChainBuilder {
        private FunctionChainStage stage = FunctionChainStage.UNSPECIFIED;
        private String name = "";
        private final List<FunctionChainOp> ops = new ArrayList<>();

        private FunctionChainBuilder() {
        }

        /**
         * Sets the execution stage where this function chain runs.
         *
         * @param stage the execution stage
         * @return this builder
         * @throws MilvusClientException if the stage is {@code null}
         */
        public FunctionChainBuilder stage(FunctionChainStage stage) {
            if (stage == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Function chain stage must not be null");
            }
            this.stage = stage;
            return this;
        }

        /**
         * Sets the name of this function chain.
         *
         * @param name the chain name
         * @return this builder
         */
        public FunctionChainBuilder name(String name) {
            this.name = name == null ? "" : name;
            return this;
        }

        /**
         * Adds a {@code map} operation that computes a new output column from an
         * expression.
         *
         * @param output the name of the output column produced by the expression
         * @param expr   the expression to evaluate
         * @return this builder
         */
        public FunctionChainBuilder map(String output, FunctionChainExpr expr) {
            this.ops.add(FunctionChainOp.map(output, expr));
            return this;
        }

        /**
         * Adds a {@code sort} operation that sorts the results by the given column.
         *
         * @param by           the column to sort by
         * @param desc         {@code true} to sort in descending order
         * @param tieBreakCol  the column used to break ties, or {@code null} to omit it
         * @return this builder
         */
        public FunctionChainBuilder sort(String by, boolean desc, String tieBreakCol) {
            this.ops.add(FunctionChainOp.sort(by, desc, tieBreakCol));
            return this;
        }

        /**
         * Adds a {@code limit} operation that keeps only the given number of results,
         * starting from the beginning.
         *
         * @param limit the maximum number of results to keep
         * @return this builder
         */
        public FunctionChainBuilder limit(int limit) {
            this.ops.add(FunctionChainOp.limit(limit, 0));
            return this;
        }

        /**
         * Adds a {@code limit} operation that keeps only the given number of results,
         * skipping the given number of leading results.
         *
         * @param limit  the maximum number of results to keep
         * @param offset the number of results to skip
         * @return this builder
         */
        public FunctionChainBuilder limit(int limit, int offset) {
            this.ops.add(FunctionChainOp.limit(limit, offset));
            return this;
        }

        /**
         * Builds the {@link FunctionChain}.
         *
         * @return the built function chain
         */
        public FunctionChain build() {
            return new FunctionChain(this);
        }
    }
}
