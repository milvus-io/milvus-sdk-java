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

package io.milvus.tutorial.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import java.util.Arrays;

/**
 * Tutorial 3: Schema design.
 *
 * <p>Shows the field data types that a collection schema can declare: scalar and container types
 * in one collection, vector types in another, and sparse / struct fields in a third. Each schema is
 * created and then read back with {@code describeCollection} so the server's representation can be
 * inspected.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String scalarCollection = "tutorial_schema_scalar";
            String vectorCollection = "tutorial_schema_vector";
            String structCollection = "tutorial_schema_struct";

            // Pre-drop any collections left behind by an interrupted run; dropping non-existent
            // collections succeeds, so this keeps the tutorial rerunnable.
            for (String name : Arrays.asList(scalarCollection, vectorCollection, structCollection)) {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(name)
                        .build());
            }

            createAndDescribe(client, scalarCollection, "Scalar and container data types", scalarSchema());
            createAndDescribe(client, vectorCollection, "Vector data types", vectorSchema());
            createAndDescribe(client, structCollection, "Sparse and struct data types", structSchema());

            for (String name : Arrays.asList(scalarCollection, vectorCollection, structCollection)) {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(name)
                        .build());
                System.out.printf("Collection '%s' dropped%n", name);
            }
        } finally {
            client.close();
        }
    }

    private static CreateCollectionReq.CollectionSchema scalarSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("bool_value")
                .dataType(DataType.Bool)
                .isNullable(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("int8_value")
                .dataType(DataType.Int8)
                .defaultValue((short) 0)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("int32_value")
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("int64_value")
                .dataType(DataType.Int64)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("float_value")
                .dataType(DataType.Float)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("double_value")
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("varchar_value")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("json_value")
                .dataType(DataType.JSON)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("timestamp_value")
                .dataType(DataType.Timestamptz)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("int64_array")
                .dataType(DataType.Array)
                .elementType(DataType.Int64)
                .maxCapacity(32)
                .build());
        return schema;
    }

    private static CreateCollectionReq.CollectionSchema vectorSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(8)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(64)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("float16_vector")
                .dataType(DataType.Float16Vector)
                .dimension(8)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("bfloat16_vector")
                .dataType(DataType.BFloat16Vector)
                .dimension(8)
                .build());
        return schema;
    }

    private static CreateCollectionReq.CollectionSchema structSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("int8_vector")
                .dataType(DataType.Int8Vector)
                .dimension(8)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("events")
                .description("An entity may contain up to 16 event records")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(16)
                .addStructField(AddFieldReq.builder()
                        .fieldName("label")
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("position")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("embedding")
                        .dataType(DataType.FloatVector)
                        .dimension(8)
                        .build())
                .build());
        return schema;
    }

    private static void createAndDescribe(MilvusClientV2 client, String collectionName,
                                          String heading, CreateCollectionReq.CollectionSchema schema) {
        System.out.println("\n" + heading);
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .description(heading)
                .collectionSchema(schema)
                .build());
        System.out.printf("Collection '%s' created%n", collectionName);

        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        CreateCollectionReq.CollectionSchema described = resp.getCollectionSchema();
        for (CreateCollectionReq.FieldSchema field : described.getFieldSchemaList()) {
            StringBuilder line = new StringBuilder();
            line.append("  field=").append(String.format("%-18s", field.getName()))
                    .append(" type=").append(field.getDataType());
            if (field.getDimension() != null && field.getDimension() > 0) {
                line.append(" dim=").append(field.getDimension());
            }
            if (field.getElementType() != null) {
                line.append(" element_type=").append(field.getElementType())
                        .append(" max_capacity=").append(field.getMaxCapacity());
            }
            if (Boolean.TRUE.equals(field.getIsPrimaryKey())) {
                line.append(" primary_key");
            }
            if (Boolean.TRUE.equals(field.getIsNullable())) {
                line.append(" nullable");
            }
            if (field.getDefaultValue() != null) {
                line.append(" default=").append(field.getDefaultValue());
            }
            System.out.println(line);
        }
        for (CreateCollectionReq.StructFieldSchema structField : described.getStructFields()) {
            System.out.printf("  field=%-18s type=Array element_type=Struct max_capacity=%d%n",
                    structField.getName(), structField.getMaxCapacity());
            for (CreateCollectionReq.FieldSchema subField : structField.getFields()) {
                System.out.printf("    sub_field=%-14s type=%s", subField.getName(), subField.getDataType());
                if (subField.getDimension() != null && subField.getDimension() > 0) {
                    System.out.printf(" dim=%d", subField.getDimension());
                }
                System.out.println();
            }
        }
    }
}
