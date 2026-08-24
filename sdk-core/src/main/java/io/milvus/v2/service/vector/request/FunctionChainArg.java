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

/**
 * An argument of a function-chain expression: either a collection-field reference or a literal
 * value. Mirrors PyMilvus's {@code FunctionChainArg} union of {@code ColumnRef} and literal.
 */
public class FunctionChainArg {
    private final String columnName;
    private final FunctionParamValue literal;

    private FunctionChainArg(String columnName, FunctionParamValue literal) {
        this.columnName = columnName;
        this.literal = literal;
    }

    public static FunctionChainArg col(String name) {
        if (name == null || name.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Column name must not be empty");
        }
        return new FunctionChainArg(name, null);
    }

    public static FunctionChainArg literal(Object value) {
        return new FunctionChainArg(null, FunctionParamValue.from(value));
    }

    public boolean isColumn() {
        return columnName != null;
    }

    public String getColumnName() {
        return columnName;
    }

    public FunctionParamValue getLiteral() {
        return literal;
    }

    public io.milvus.grpc.FunctionChainExprArg toGrpc() {
        if (columnName != null) {
            return io.milvus.grpc.FunctionChainExprArg.newBuilder()
                    .setColumn(io.milvus.grpc.FunctionChainColumnArg.newBuilder().setName(columnName).build())
                    .build();
        }
        return io.milvus.grpc.FunctionChainExprArg.newBuilder()
                .setLiteral(literal.toGrpc())
                .build();
    }
}
