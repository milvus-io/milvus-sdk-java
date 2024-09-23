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

package io.milvus.common.utils;

import io.milvus.exception.ParamException;
import io.milvus.param.collection.FieldType;
import io.milvus.response.DescCollResponseWrapper;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class VectorUtils {
    public static String convertPksExpr(List<?> primaryIds, DescCollResponseWrapper wrapper) {
        Optional<FieldType> optional = wrapper.getFields().stream().filter(FieldType::isPrimaryKey).findFirst();
        String expr;
        if (optional.isPresent()) {
            FieldType primaryField = optional.get();
            switch (primaryField.getDataType()) {
                case Int64:
                    List<String> primaryStringIds = primaryIds.stream().map(String::valueOf).collect(Collectors.toList());
                    expr = convertPksExpr(primaryStringIds, primaryField.getName());
                    break;
                case VarChar:
                    List<String> primaryVarcharIds = primaryIds.stream().map(primaryId -> String.format("\"%s\"", primaryId)).collect(Collectors.toList());
                    expr = convertPksExpr(primaryVarcharIds, primaryField.getName());
                    break;
                default:
                    throw new ParamException("The primary key is not of type int64 or varchar, and the current operation is not supported.");
            }
        } else {
            throw new ParamException("No primary key found.");
        }
        return expr;
    }

    public static String convertPksExpr(List<?> primaryIds, String primaryFieldName) {
        String strIDs = primaryIds.stream().map(Object::toString).collect(Collectors.joining(","));
        return primaryFieldName + " in [" + strIDs + "]";
    }
}
