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

package io.milvus.bulkwriter.common.utils;

import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.List;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public class ParquetUtils {
    public static MessageType parseCollectionSchema(CollectionSchemaParam collectionSchema) {
        List<FieldType> fieldTypes = collectionSchema.getFieldTypes();
        Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
        for (FieldType fieldType : fieldTypes) {
            if (fieldType.isAutoID()) {
                continue;
            }
            switch (fieldType.getDataType()) {
                case FloatVector:
                    messageTypeBuilder.requiredList()
                            .requiredElement(PrimitiveType.PrimitiveTypeName.FLOAT)
                            .named(fieldType.getName());
                    break;
                case BinaryVector:
                    messageTypeBuilder.requiredList()
                            .requiredElement(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, false))
                            .named(fieldType.getName());
                    break;
                case Array:
                    fillArrayType(messageTypeBuilder, fieldType);
                    break;

                case Int64:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.INT64)
                            .named(fieldType.getName());
                    break;
                case VarChar:
                case JSON:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                            .named(fieldType.getName());
                    break;
                case Int8:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, true))
                            .named(fieldType.getName());
                    break;
                case Int16:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16, true))
                            .named(fieldType.getName());
                    break;
                case Int32:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.INT32)
                            .named(fieldType.getName());
                    break;
                case Float:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                            .named(fieldType.getName());
                    break;
                case Double:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                            .named(fieldType.getName());
                    break;
                case Bool:
                    messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                            .named(fieldType.getName());
                    break;

            }
        }

        if (collectionSchema.isEnableDynamicField()) {
            messageTypeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                    .named(DYNAMIC_FIELD_NAME);
        }
        return messageTypeBuilder.named("schema");
    }

    private static void fillArrayType(Types.MessageTypeBuilder messageTypeBuilder, FieldType fieldType) {
        switch (fieldType.getElementType()) {
            case Int64:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.INT64)
                        .named(fieldType.getName());
                break;
            case VarChar:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                        .named(fieldType.getName());
                break;
            case Int8:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, true))
                        .named(fieldType.getName());
                break;
            case Int16:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16, true))
                        .named(fieldType.getName());
                break;
            case Int32:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.INT32)
                        .named(fieldType.getName());
                break;
            case Float:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .named(fieldType.getName());
                break;
            case Double:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(fieldType.getName());
                break;
            case Bool:
                messageTypeBuilder.requiredList()
                        .requiredElement(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                        .named(fieldType.getName());
                break;

        }
    }
}
