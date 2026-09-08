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

package io.milvus.unit.v1.param;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.FieldType;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CreateSimpleCollectionParamTest {
    @Test
    void buildAndGetDefaults() {
        CreateSimpleCollectionParam param = CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .build();

        assertEquals("coll1", param.getCreateCollectionParam().getCollectionName());
        assertEquals("", param.getCreateCollectionParam().getDescription());
        assertEquals(ConsistencyLevelEnum.BOUNDED, param.getCreateCollectionParam().getConsistencyLevel());

        List<FieldType> fieldTypes = param.getCreateCollectionParam().getFieldTypes();
        assertEquals(2, fieldTypes.size());
        assertEquals(Constant.PRIMARY_FIELD_NAME_DEFAULT, fieldTypes.get(0).getName());
        assertEquals(DataType.Int64, fieldTypes.get(0).getDataType());
        assertTrue(fieldTypes.get(0).isPrimaryKey());
        assertEquals(Constant.VECTOR_FIELD_NAME_DEFAULT, fieldTypes.get(1).getName());
        assertEquals(DataType.FloatVector, fieldTypes.get(1).getDataType());

        assertEquals("coll1", param.getCreateIndexParam().getCollectionName());
        assertEquals(Constant.VECTOR_FIELD_NAME_DEFAULT, param.getCreateIndexParam().getFieldName());
        assertEquals(Constant.VECTOR_INDEX_NAME_DEFAULT, param.getCreateIndexParam().getIndexName());
        assertEquals(IndexType.AUTOINDEX, param.getCreateIndexParam().getIndexType());
        assertEquals("L2", param.getCreateIndexParam().getExtraParam().get(Constant.METRIC_TYPE));

        assertEquals("coll1", param.getLoadCollectionParam().getCollectionName());
        assertTrue(param.getLoadCollectionParam().isSyncLoad());
    }

    @Test
    void buildWithAllFields() {
        CreateSimpleCollectionParam param = CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(16)
                .withMetricType(MetricType.COSINE)
                .withDescription("desc")
                .withPrimaryField("id")
                .withVectorField("vec")
                .withAutoId(false)
                .withSyncLoad(false)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withPrimaryFieldType(DataType.Int64)
                .withMaxLength(100)
                .build();

        assertEquals("desc", param.getCreateCollectionParam().getDescription());
        assertEquals(ConsistencyLevelEnum.STRONG, param.getCreateCollectionParam().getConsistencyLevel());
        assertEquals("id", param.getCreateCollectionParam().getFieldTypes().get(0).getName());
        assertEquals("vec", param.getCreateCollectionParam().getFieldTypes().get(1).getName());
        assertEquals("COSINE", param.getCreateIndexParam().getExtraParam().get(Constant.METRIC_TYPE));
        assertEquals("vec", param.getCreateIndexParam().getFieldName());
        assertEquals(false, param.getLoadCollectionParam().isSyncLoad());
    }

    @Test
    void buildWithVarCharPrimaryField() {
        CreateSimpleCollectionParam param = CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .withPrimaryFieldType(DataType.VarChar)
                .withMaxLength(64)
                .build();

        assertEquals(DataType.VarChar, param.getCreateCollectionParam().getFieldTypes().get(0).getDataType());
        assertEquals("64", param.getCreateCollectionParam().getFieldTypes().get(0)
                .getTypeParams().get(Constant.VARCHAR_MAX_LENGTH));
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withDimension(8).build());
    }

    @Test
    void buildFailsWhenDimensionInvalid() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(0)
                .build());
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(-1)
                .build());
    }

    @Test
    void buildFailsWhenPrimaryFieldTypeInvalid() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .withPrimaryFieldType(DataType.FloatVector)
                .build());
    }

    @Test
    void buildFailsWhenVarCharWithoutMaxLength() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .withPrimaryFieldType(DataType.VarChar)
                .build());
    }

    @Test
    void buildFailsWhenVarCharMaxLengthInvalid() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .withPrimaryFieldType(DataType.VarChar)
                .withMaxLength(0)
                .build());
    }

    @Test
    void buildFailsWhenVarCharWithAutoId() {
        assertThrows(ParamException.class, () -> CreateSimpleCollectionParam.newBuilder()
                .withCollectionName("coll1")
                .withDimension(8)
                .withPrimaryFieldType(DataType.VarChar)
                .withMaxLength(64)
                .withAutoId(true)
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withMetricType(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withDescription(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withPrimaryField(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withVectorField(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withConsistencyLevel(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withPrimaryFieldType(null));
        assertThrows(IllegalArgumentException.class, () ->
                CreateSimpleCollectionParam.newBuilder().withMaxLength(null));
    }
}
