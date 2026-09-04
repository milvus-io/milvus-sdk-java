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

package io.milvus.support.v1;


import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.support.TestUtils;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.*;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.alias.ListAliasesParam;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.WeightedRanker;
import io.milvus.param.highlevel.dml.DeleteIdsParam;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.param.highlevel.dml.response.DeleteResponse;
import io.milvus.param.highlevel.dml.response.GetResponse;
import io.milvus.param.index.*;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.response.*;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public abstract class MilvusV1DockerTestBase {
    protected static MilvusClient client;
    protected static RandomStringGenerator generator;
    protected static final int DIMENSION = 256;
    protected static final Random RANDOM = new Random();
    protected static final int ARRAY_CAPACITY = 100;
    protected static final float FLOAT16_PRECISION = 0.001f;
    protected static final float BFLOAT16_PRECISION = 0.01f;
    protected static final TestUtils utils = new TestUtils(DIMENSION);
    protected static final File DockerComposeFile = TestUtils.dockerComposeFile("docker-compose.yml");
    protected static final File DockerComposeVolumeDirectory = new File("target/milvus-compose");
    protected static final List<String> DockerComposeContainerNames = Arrays.asList("milvus-javasdk-etcd", "milvus-javasdk-minio", "milvus-javasdk-standalone");
    protected static class MilvusClientForTest extends MilvusServiceClient {
        public MilvusClientForTest(ConnectParam connectParam) {
            super(connectParam);
        }

        public DescribeCollectionResponse getCollectionInfo(String databaseName, String collectionName) {
            return SchemaCache.getInstance().get(currentEndpoint(), databaseName, collectionName);
        }
    }

    // All split system-test classes in one JVM share a single Milvus standalone.
    // TestUtils.startMilvusStandalone is idempotent per compose file and the
    // stack is torn down once at JVM exit by a shutdown hook, so the server is
    // not restarted for every test class.
    @BeforeAll
    public static void setUp() {
        TestUtils.startMilvusStandalone(DockerComposeFile, DockerComposeVolumeDirectory, DockerComposeContainerNames);

        ConnectParam connectParam = connectParamBuilder()
                .withAuthorization("root", "Milvus")
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(10)
                .build();
        client = new MilvusServiceClient(connectParam).withRetry(retryParam).withTimeout(10, TimeUnit.SECONDS);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterAll
    public static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    protected static ConnectParam.Builder connectParamBuilder() {
        return connectParamBuilder(TestUtils.MilvusStandaloneUri);
    }

    protected static ConnectParam.Builder connectParamBuilder(String milvusUri) {
        return ConnectParam.newBuilder().withUri(milvusUri);
    }

    protected CollectionSchemaParam buildSchema(boolean strID, boolean autoID, boolean enabledDynamicSchema, List<DataType> fieldTypes) {
        CollectionSchemaParam.Builder builder = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(enabledDynamicSchema);

        if (strID) {
            builder.addFieldType(FieldType.newBuilder()
                    .withPrimaryKey(true)
                    .withDataType(DataType.VarChar)
                    .withMaxLength(200)
                    .withName("id")
                    .build());
        } else {
            builder.addFieldType(FieldType.newBuilder()
                    .withPrimaryKey(true)
                    .withAutoID(autoID)
                    .withDataType(DataType.Int64)
                    .withName("id")
                    .build());
        }

        for (DataType dataType : fieldTypes) {
            if (dataType == DataType.Array) {
                builder.addFieldType(FieldType.newBuilder()
                        .withDataType(dataType)
                        .withName(dataType.name() + "_int32")
                        .withElementType(DataType.Int32)
                        .withMaxCapacity(ARRAY_CAPACITY)
                        .build());
                builder.addFieldType(FieldType.newBuilder()
                        .withDataType(dataType)
                        .withName(dataType.name() + "_float")
                        .withElementType(DataType.Float)
                        .withMaxCapacity(ARRAY_CAPACITY)
                        .build());
                builder.addFieldType(FieldType.newBuilder()
                        .withDataType(dataType)
                        .withName(dataType.name() + "_varchar")
                        .withElementType(DataType.VarChar)
                        .withMaxLength(200)
                        .withMaxCapacity(ARRAY_CAPACITY)
                        .build());
            } else {
                FieldType.Builder fieldBuilder = FieldType.newBuilder()
                        .withDataType(dataType)
                        .withName(dataType.name());

                if (dataType == DataType.VarChar) {
                    fieldBuilder.withMaxLength(60000);
                } else if (ParamUtils.isVectorDataType(dataType) && dataType != DataType.SparseFloatVector) {
                    fieldBuilder.withDimension(DIMENSION);
                }

                builder.addFieldType(fieldBuilder.build());
            }
        }

        return builder.build();
    }

    protected List<InsertParam.Field> generateColumnsData(CollectionSchemaParam schema, int count, int idStart) {
        List<InsertParam.Field> columns = new ArrayList<>();
        List<FieldType> fieldTypes = schema.getFieldTypes();
        for (FieldType fieldType : fieldTypes) {
            if (fieldType.isAutoID()) {
                continue;
            }
            switch (fieldType.getDataType()) {
                case Bool: {
                    List<Boolean> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add(i % 3 == 0);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Int8:
                case Int16: {
                    List<Short> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((short) (i % 128));
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Int32: {
                    List<Integer> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add(i);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Int64: {
                    List<Long> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((long) i);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Float: {
                    List<Float> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((float) i / 3);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Double: {
                    List<Double> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((double) i / 7);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case VarChar: {
                    List<String> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add(String.format("varchar_%d", i));
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case JSON: {
                    List<JsonObject> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        JsonObject info = new JsonObject();
                        info.addProperty("json", i);
                        data.add(info);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Array: {
                    List<List<?>> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add(utils.generateRandomArray(fieldType.getElementType(), fieldType.getMaxCapacity()));
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case FloatVector: {
                    List<List<Float>> data = utils.generateFloatVectors(count);
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case BinaryVector: {
                    List<ByteBuffer> data = utils.generateBinaryVectors(count);
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Float16Vector: {
                    List<ByteBuffer> data = utils.generateFloat16Vectors(count);
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case BFloat16Vector: {
                    List<ByteBuffer> data = utils.generateBFloat16Vectors(count);
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case SparseFloatVector: {
                    List<SortedMap<Long, Float>> data = utils.generateSparseVectors(count);
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                default:
                    Assertions.fail();
            }
        }

        if (schema.isEnableDynamicField()) {
            List<JsonObject> data = new ArrayList<>();
            for (int i = idStart; i < idStart + count; ++i) {
                JsonObject info = new JsonObject();
                info.addProperty("dynamic", i);
                data.add(info);
            }
            columns.add(new InsertParam.Field(Constant.DYNAMIC_FIELD_NAME, data));
        }
        return columns;
    }

    protected List<JsonObject> generateRowsData(CollectionSchemaParam schema, int count, int idStart) {
        List<JsonObject> rows = new ArrayList<>();
        List<FieldType> fieldTypes = schema.getFieldTypes();
        for (int i = idStart; i < idStart + count; ++i) {
            JsonObject row = new JsonObject();
            for (FieldType fieldType : fieldTypes) {
                if (fieldType.isAutoID()) {
                    continue;
                }
                switch (fieldType.getDataType()) {
                    case Bool:
                        row.addProperty(fieldType.getName(), i % 3 == 0);
                        break;
                    case Int8:
                    case Int16:
                        row.addProperty(fieldType.getName(), (short) (i % 128));
                        break;
                    case Int32:
                        row.addProperty(fieldType.getName(), i);
                        break;
                    case Int64:
                        row.addProperty(fieldType.getName(), (long) i);
                        break;
                    case Float:
                        row.addProperty(fieldType.getName(), (float) i / 3);
                        break;
                    case Double:
                        row.addProperty(fieldType.getName(), (float) i / 7);
                        break;
                    case VarChar:
                        row.addProperty(fieldType.getName(), String.format("varchar_%d", i));
                        break;
                    case JSON:
                        JsonObject info = new JsonObject();
                        info.addProperty("json", i);
                        row.add(fieldType.getName(), info);
                        break;
                    case Array:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateRandomArray(fieldType.getElementType(), fieldType.getMaxCapacity())));
                        break;
                    case FloatVector:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateFloatVector()));
                        break;
                    case BinaryVector:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateBinaryVector().array()));
                        break;
                    case Float16Vector:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateFloat16Vector().array()));
                        break;
                    case BFloat16Vector:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateBFloat16Vector().array()));
                        break;
                    case SparseFloatVector:
                        row.add(fieldType.getName(), JsonUtils.toJsonTree(utils.generateSparseVector()));
                        break;
                    default:
                        Assertions.fail();
                }
            }
            if (schema.isEnableDynamicField()) {
                row.addProperty("dynamic", i);
            }
            rows.add(row);
        }
        return rows;
    }

    protected static void testIndex(String collectionName, String fieldName,
                                  IndexType type, MetricType metric,
                                  String params, Boolean syncMode) {
        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(fieldName)
                .withIndexName("index")
                .withIndexType(type)
                .withMetricType(metric)
                .withExtraParam(params)
                .withSyncMode(syncMode)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // drop index
        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());
    }

    protected static void highLevelCreateCollection(FieldType primaryField, FieldType vectorField, String randomCollectionName) {
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .addFieldType(primaryField)
                .addFieldType(vectorField)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(vectorField.getName())
                .withIndexName("abv")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());
    }

    protected static void testHighLevelGet(String collectionName, List primaryIds) {
        GetIdsParam getIdsParam = GetIdsParam.newBuilder()
                .withCollectionName(collectionName)
                .withPrimaryIds(primaryIds)
                .build();

        R<GetResponse> getResponseR = client.get(getIdsParam);
        String outPutStr = String.format("collectionName:%s, primaryIds:%s, getResponseR:%s", collectionName, primaryIds, getResponseR.getData());
        System.out.println(outPutStr);
        Assertions.assertEquals(R.Status.Success.getCode(), getResponseR.getStatus().intValue());
    }

    protected static void testHighLevelDelete(String collectionName, List primaryIds) {
        DeleteIdsParam deleteIdsParam = DeleteIdsParam.newBuilder()
                .withCollectionName(collectionName)
                .withPrimaryIds(primaryIds)
                .build();

        R<DeleteResponse> deleteResponseR = client.delete(deleteIdsParam);
        String outPutStr = String.format("collectionName:%s, primaryIds:%s, deleteResponseR:%s", collectionName, primaryIds, deleteResponseR);
        System.out.println(outPutStr);
        Assertions.assertEquals(R.Status.Success.getCode(), deleteResponseR.getStatus().intValue());
    }

    protected static void createSimpleCollection(MilvusClient client, String dbName, String collName, String pkName,
                                               boolean autoID, int dimension, ConsistencyLevelEnum level) {
        client.dropCollection(DropCollectionParam.newBuilder()
                .withDatabaseName(dbName)
                .withCollectionName(collName)
                .build());

        // collection schema
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(autoID)
                .withDataType(DataType.Int64)
                .withName(pkName)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(dimension)
                .build());

        // create collection
        R<RpcStatus> createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withDatabaseName(dbName)
                .withCollectionName(collName)
                .withFieldTypes(fieldsSchema)
                .withConsistencyLevel(level)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withDatabaseName(dbName)
                .withCollectionName(collName)
                .withFieldName("vector")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withDatabaseName(dbName)
                .withCollectionName(collName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());
    }

}
