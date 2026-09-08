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

package io.milvus.integration.v2.service.collection;

import io.milvus.support.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
class CollectionSchemaVerifyTest extends BaseTest {

    @Test
    void testCollectionSchemaVerifyRejectsMissingPrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Primary key"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsMultiplePrimaryKeys() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id1").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("id2").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("more than one primary key"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsUnsupportedPrimaryKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Float).isPrimaryKey(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Int64 or VarChar"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsPartitionKeyEqualToPrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).isPartitionKey(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("cannot be the primary key"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsMultiplePartitionKeys() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("p1").dataType(DataType.Int64).isPartitionKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("p2").dataType(DataType.Int64).isPartitionKey(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("more than one partition key"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsMultipleClusteringKeys() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("c1").dataType(DataType.Int64).isClusteringKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("c2").dataType(DataType.Int64).isClusteringKey(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("more than one clustering key"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsAutoIdOnNonPrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build());
        schema.addField(AddFieldReq.builder().fieldName("other").dataType(DataType.Int64).autoID(Boolean.TRUE).build());

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("auto_id"));
    }

    @Test
    void testCollectionSchemaVerifySkipsExternalCollection() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.setExternalSource("s3://bucket/path");
        schema.addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build());

        Assertions.assertDoesNotThrow(schema::verify);
    }

    @Test
    void testCollectionSchemaVerifyRejectsNullFieldSchemaList() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(null)
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Schema fields cannot be null"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsNullPrimaryKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(
                        CreateCollectionReq.FieldSchema.builder().name("id").isPrimaryKey(Boolean.TRUE).build()))
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Primary key field data type is required"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsNullPartitionKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build(),
                        CreateCollectionReq.FieldSchema.builder().name("pk").isPartitionKey(Boolean.TRUE).build()))
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Partition key field data type is required"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsUnsupportedPartitionKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build(),
                        CreateCollectionReq.FieldSchema.builder().name("pk").dataType(DataType.Float).isPartitionKey(Boolean.TRUE).build()))
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Partition key data type must be Int64 or VarChar"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsNullClusteringKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build(),
                        CreateCollectionReq.FieldSchema.builder().name("ck").isClusteringKey(Boolean.TRUE).build()))
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Clustering key field data type is required"));
    }

    @Test
    void testCollectionSchemaVerifyRejectsUnsupportedClusteringKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).build(),
                        CreateCollectionReq.FieldSchema.builder().name("ck").dataType(DataType.Bool).isClusteringKey(Boolean.TRUE).build()))
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class, schema::verify);
        Assertions.assertTrue(exception.getMessage().contains("Unsupported clustering key data type"));
    }

    @Test
    void testCreateCollectionRejectsNullIdType() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        req.setIdType(null);

        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> client_v2.createCollection(req));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Primary key type is required"));
    }

    @Test
    void testCreateCollectionRejectsUnsupportedIdType() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .idType(DataType.Float)
                .build();

        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> client_v2.createCollection(req));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Primary key type must be Int64 or VarChar"));
    }
}
