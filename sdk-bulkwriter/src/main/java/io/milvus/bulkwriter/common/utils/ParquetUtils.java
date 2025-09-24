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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.List;

import static io.milvus.param.Constant.DYNAMIC_FIELD_NAME;

public class ParquetUtils {
    private static void setMessageType(Types.MessageTypeBuilder builder,
                                       PrimitiveType.PrimitiveTypeName primitiveName,
                                       LogicalTypeAnnotation logicType,
                                       CreateCollectionReq.FieldSchema field,
                                       boolean isListType) {
        // Note:
        // Ideally, if the field is nullable, the builder should be builder.requiredList() or builder.required().
        // But in milvus (versions <= v2.5.4), the milvus server logic cannot handle parquet files with
        // requiredList()/required(), the server will crash in the file /internal/util/importutilv2/parquet/field_reader.go,
        // in the parquet.FieldReader.Next() with a runtime error: "index out of range [0] with length 0".
        // This issue is tracked by https://github.com/milvus-io/milvus/issues/40291
        // The python sdk BulkWriter uses Pandas to generate parquet files, the Pandas sets all schema to be "optional"
        // so that the crash is by-passed.
        // To avoid the crash, in Java SDK, we use optionalList()/optional() even if the field is nullable.
        if (isListType) {
            // FloatVector/BinaryVector/Float16Vector/BFloat16Vector/Array enter this section
            if (logicType == null) {
                builder.optionalList().optionalElement(primitiveName).named(field.getName());
            } else {
                builder.optionalList().optionalElement(primitiveName).as(logicType).named(field.getName());
            }
        } else {
            // SparseFloatVector/Bool/Int8/Int16/Int32/Int64/Float/Double/Varchar/JSON enter this section
            if (logicType == null) {
                builder.optional(primitiveName).named(field.getName());
            } else {
                builder.optional(primitiveName).as(logicType).named(field.getName());
            }
        }
    }

    public static MessageType parseCollectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
        List<CreateCollectionReq.FieldSchema> fields = collectionSchema.getFieldSchemaList();
        List<String> outputFieldNames = V2AdapterUtils.getOutputFieldNames(collectionSchema);
        Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
        for (CreateCollectionReq.FieldSchema field : fields) {
            if (field.getIsPrimaryKey() && field.getAutoID()) {
                continue;
            }
            if (outputFieldNames.contains(field.getName())) {
                continue;
            }

            switch (field.getDataType()) {
                case FloatVector:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.FLOAT, null, field, true);
                    break;
                case BinaryVector:
                case Float16Vector:
                case BFloat16Vector:
                case Int8Vector:
                    boolean isSigned = (field.getDataType() == io.milvus.v2.common.DataType.Int8Vector);
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32,
                            LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, isSigned), field, true);
                    break;
                case Array:
                    fillArrayType(messageTypeBuilder, field);
                    break;

                case Int64:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT64, null, field, false);
                    break;
                case VarChar:
                case Timestamptz:
                case JSON:
                case SparseFloatVector: // sparse vector is parsed as JSON format string in the server side
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY,
                            LogicalTypeAnnotation.stringType(), field, false);
                    break;
                case Int8:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32,
                            LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, true), field, false);
                    break;
                case Int16:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32,
                            LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16, true), field, false);
                    break;
                case Int32:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32, null, field, false);
                    break;
                case Float:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.FLOAT, null, field, false);
                    break;
                case Double:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.DOUBLE, null, field, false);
                    break;
                case Bool:
                    setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BOOLEAN, null, field, false);
                    break;

            }
        }

        if (collectionSchema.isEnableDynamicField()) {
            messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                    .named(DYNAMIC_FIELD_NAME);
        }
        return messageTypeBuilder.named("schema");
    }

    private static void fillArrayType(Types.MessageTypeBuilder messageTypeBuilder, CreateCollectionReq.FieldSchema field) {
        switch (field.getElementType()) {
            case Int64:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT64, null, field, true);
                break;
            case VarChar:
            case Timestamptz:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY,
                        LogicalTypeAnnotation.stringType(), field, true);
                break;
            case Int8:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32,
                        LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(8, true), field, true);
                break;
            case Int16:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32,
                        LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16, true), field, true);
                break;
            case Int32:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT32, null, field, true);
                break;
            case Float:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.FLOAT, null, field, true);
                break;
            case Double:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.DOUBLE, null, field, true);
                break;
            case Bool:
                setMessageType(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BOOLEAN, null, field, true);
                break;
        }
    }

    public static Configuration getParquetConfiguration() {
        // set fs.file.impl.disable.cache to true for this issue: https://github.com/milvus-io/milvus-sdk-java/issues/1381
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl.disable.cache", "true");
        return configuration;
    }
}
