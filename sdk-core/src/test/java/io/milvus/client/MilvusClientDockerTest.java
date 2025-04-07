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

package io.milvus.client;

import com.google.gson.*;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.TestUtils;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.JsonUtils;
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
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.response.*;

import org.apache.commons.text.RandomStringGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.milvus.MilvusContainer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Testcontainers(disabledWithoutDocker = true)
class MilvusClientDockerTest {
    private static MilvusClient client;
    private static RandomStringGenerator generator;
    private static final int DIMENSION = 256;
    private static final int ARRAY_CAPACITY = 100;
    private static final float FLOAT16_PRECISION = 0.001f;
    private static final float BFLOAT16_PRECISION = 0.01f;

    private static final TestUtils utils = new TestUtils(DIMENSION);

    @Container
    private static final MilvusContainer milvus = new MilvusContainer("milvusdb/milvus:v2.5.8");

    @BeforeAll
    public static void setUp() {
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
        return connectParamBuilder(milvus.getEndpoint());
    }

    private static ConnectParam.Builder connectParamBuilder(String milvusUri) {
        return ConnectParam.newBuilder().withUri(milvusUri);
    }

    private CollectionSchemaParam buildSchema(boolean strID, boolean autoID, boolean enabledDynamicSchema, List<DataType> fieldTypes) {
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

    private List<InsertParam.Field> generateColumnsData(CollectionSchemaParam schema, int count, int idStart) {
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
                        data.add(i%3==0 ? true : false);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Int8:
                case Int16: {
                    List<Short> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((short) (i%128));
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
                        data.add((long)i);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Float: {
                    List<Float> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((float)i/3);
                    }
                    columns.add(new InsertParam.Field(fieldType.getName(), data));
                    break;
                }
                case Double: {
                    List<Double> data = new ArrayList<>();
                    for (int i = idStart; i < idStart + count; ++i) {
                        data.add((double)i/7);
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

    private List<JsonObject> generateRowsData(CollectionSchemaParam schema, int count, int idStart) {
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
                        row.addProperty(fieldType.getName(), (short)(i%128));
                        break;
                    case Int32:
                        row.addProperty(fieldType.getName(), i);
                        break;
                    case Int64:
                        row.addProperty(fieldType.getName(), (long)i);
                        break;
                    case Float:
                        row.addProperty(fieldType.getName(), (float)i/3);
                        break;
                    case Double:
                        row.addProperty(fieldType.getName(), (float)i/7);
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

    @Test
    void testFloatVectors() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Arrays.asList(DataType.FloatVector, DataType.Bool, DataType.Double, DataType.Int8));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .withShardsNum(3)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withReplicaNumber(1)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper collDescWrapper = new DescCollResponseWrapper(response.getData());
        Assertions.assertEquals(randomCollectionName, collDescWrapper.getCollectionName());
        Assertions.assertEquals("default", collDescWrapper.getDatabaseName());
        Assertions.assertEquals("test", collDescWrapper.getCollectionDescription());
        Assertions.assertEquals(3, collDescWrapper.getShardNumber());
        Assertions.assertEquals(schema.getFieldTypes().size(), collDescWrapper.getFields().size());
        Assertions.assertEquals(1, collDescWrapper.getVectorFields().size());
        FieldType primaryField = collDescWrapper.getPrimaryField();
        Assertions.assertFalse(primaryField.isAutoID());
        CollectionSchemaParam fetchSchema = collDescWrapper.getSchema();
        Assertions.assertFalse(fetchSchema.isEnableDynamicField());
        Assertions.assertEquals(ConsistencyLevelEnum.EVENTUALLY, collDescWrapper.getConsistencyLevel());
        Assertions.assertEquals(1, collDescWrapper.getReplicaNumber());
        System.out.println(collDescWrapper);

        R<ShowPartitionsResponse> spResp = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        System.out.println(spResp);

        ShowPartResponseWrapper wra = new ShowPartResponseWrapper(spResp.getData());
        List<ShowPartResponseWrapper.PartitionInfo> parts = wra.getPartitionsInfo();
        System.out.println("Partition num: "+parts.size());

        // insert data
        int rowCount = 10000;
        List<InsertParam.Field> columnsData = generateColumnsData(schema, rowCount, 0);
        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(columnsData)
                .build();

        R<MutationResult> insertR = client.withTimeout(10, TimeUnit.SECONDS).insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // get partition statistics
        R<GetPartitionStatisticsResponse> statPartR = client.getPartitionStatistics(GetPartitionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withPartitionName("_default") // each collection has '_default' partition
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statPartR.getStatus().intValue());

        GetPartStatResponseWrapper statPart = new GetPartStatResponseWrapper(statPartR.getData());
        Assertions.assertEquals(rowCount, statPart.getRowCount());
        System.out.println("Partition row count: " + statPart.getRowCount());

        // create index on scalar field
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.Int8.name())
                .withIndexType(IndexType.STL_SORT)
                .withSyncMode(Boolean.TRUE)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // create index on vector field
        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexName("abv")
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"M\":16,\"efConstruction\":64}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        DescIndexResponseWrapper indexDescWrapper = new DescIndexResponseWrapper(descIndexR.getData());
        DescIndexResponseWrapper.IndexDesc indexDesc = indexDescWrapper.getIndexDescByFieldName(DataType.FloatVector.name());
        Assertions.assertNotNull(indexDesc);
        Assertions.assertEquals(DataType.FloatVector.name(), indexDesc.getFieldName());
        Assertions.assertEquals("abv", indexDesc.getIndexName());
        Assertions.assertEquals(IndexType.HNSW, indexDesc.getIndexType());
        Assertions.assertEquals(MetricType.L2, indexDesc.getMetricType());
        Assertions.assertEquals(rowCount, indexDesc.getTotalRows());
        Assertions.assertEquals(rowCount, indexDesc.getIndexedRows());
        Assertions.assertEquals(0L, indexDesc.getPendingIndexRows());
        Assertions.assertTrue(indexDesc.getIndexFailedReason().isEmpty());
        System.out.println("Index description: " + indexDesc.toString());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // show collections
        R<ShowCollectionsResponse> showR = client.showCollections(ShowCollectionsParam.newBuilder()
                .addCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), showR.getStatus().intValue());
        ShowCollResponseWrapper info = new ShowCollResponseWrapper(showR.getData());
        System.out.println("Collection info: " + info.toString());

        // show partitions
        R<ShowPartitionsResponse> showPartR = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addPartitionName("_default") // each collection has a '_default' partition
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), showPartR.getStatus().intValue());
        ShowPartResponseWrapper infoPart = new ShowPartResponseWrapper(showPartR.getData());
        System.out.println("Partition info: " + infoPart.toString());

        // query
        Long fetchID = 100L;
        List<Float> fetchVector = (List<Float>)columnsData.get(1).getValues().get(fetchID.intValue());
        R<QueryResults> fetchR = client.query(QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("id == %d", fetchID))
                .addOutField(DataType.FloatVector.name())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), fetchR.getStatus().intValue());
        QueryResultsWrapper fetchWrapper = new QueryResultsWrapper(fetchR.getData());
        FieldDataWrapper fetchField = fetchWrapper.getFieldWrapper(DataType.FloatVector.name());
        Assertions.assertEquals(1L, fetchField.getRowCount());
        List<?> fetchObj = fetchField.getFieldData();
        Assertions.assertEquals(1, fetchObj.size());
        Assertions.assertInstanceOf(List.class, fetchObj.get(0));
        List<Float> fetchResult = (List<Float>) fetchObj.get(0);
        Assertions.assertEquals(fetchVector.size(), fetchResult.size());
        for (int i = 0; i < fetchResult.size(); i++) {
            Assertions.assertEquals(fetchVector.get(i), fetchResult.get(i));
        }

        // query vectors to verify
        List<Long> queryIDs = new ArrayList<>();
        List<Double> compareWeights = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            Assertions.assertInstanceOf(Long.class, columnsData.get(0).getValues().get(i));
            queryIDs.add((Long)columnsData.get(0).getValues().get(i));
            Assertions.assertInstanceOf(Double.class, columnsData.get(3).getValues().get(i));
            compareWeights.add((Double) columnsData.get(3).getValues().get(i));
        }
        String expr = "id in " + queryIDs;
        List<String> outputFields = Arrays.asList("id", DataType.FloatVector.name(), DataType.Bool.name(),
                DataType.Double.name(), DataType.Int8.name());
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
            Assertions.assertEquals(nq, wrapper.getFieldData().size());

            if (fieldName.compareTo("id") == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper("id").getFieldData();
                Assertions.assertEquals(nq, out.size());
                for (Object o : out) {
                    long id = (Long) o;
                    Assertions.assertTrue(queryIDs.contains(id));
                }
            }
        }

        // Note: the query() return vectors are not in same sequence to the input
        // here we cannot compare vector one by one
        // the boolean also cannot be compared
        if (outputFields.contains(DataType.FloatVector.name())) {
            Assertions.assertTrue(queryResultsWrapper.getFieldWrapper(DataType.FloatVector.name()).isVectorField());
            List<?> out = queryResultsWrapper.getFieldWrapper(DataType.FloatVector.name()).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(DataType.Bool.name())) {
            List<?> out = queryResultsWrapper.getFieldWrapper(DataType.Bool.name()).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(DataType.Double.name())) {
            List<?> out = queryResultsWrapper.getFieldWrapper(DataType.Double.name()).getFieldData();
            Assertions.assertEquals(nq, out.size());
            for (Object o : out) {
                double d = (Double) o;
                Assertions.assertTrue(compareWeights.contains(d));
            }
        }

        // query with offset and limit
        int queryLimit = 5;
        expr = DataType.Int8.name() + " > 1";
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOffset(100L)
                .withLimit((long) queryLimit)
                .build();
        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        // we didn't set the output fields, only primary key field is returned
        List<?> out = queryResultsWrapper.getFieldWrapper("id").getFieldData();
        Assertions.assertEquals(queryLimit, out.size());

        // pick some vectors to search
        List<Long> targetVectorIDs = new ArrayList<>();
        List<List<Float>> targetVectors = new ArrayList<>();
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add((Long)columnsData.get(0).getValues().get(i));
            targetVectors.add((List<Float>)columnsData.get(1).getValues().get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(topK)
                .withFloatVectors(targetVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .withParams("{\"ef\":64}")
                .addOutField(DataType.Double.name())
                .addOutField(DataType.FloatVector.name())
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            Assertions.assertEquals(targetVectorIDs.get(i), scores.get(0).getLongID());

            Object obj = scores.get(0).get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, obj);
            List<Float> outputVec = (List<Float>)obj;
            Assertions.assertEquals(targetVectors.get(i).size(), outputVec.size());
            for (int k = 0; k < outputVec.size(); k++) {
                Assertions.assertEquals(targetVectors.get(i).get(k), outputVec.get(k));
            }
        }

        List<?> fieldData = results.getFieldData(DataType.Double.name(), 0);
        Assertions.assertEquals(topK, fieldData.size());
        fieldData = results.getFieldData(DataType.Double.name(), nq - 1);
        Assertions.assertEquals(topK, fieldData.size());

        // release collection
        ReleaseCollectionParam releaseCollectionParam = ReleaseCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> releaseCollectionR = client.releaseCollection(releaseCollectionParam);
        Assertions.assertEquals(R.Status.Success.getCode(), releaseCollectionR.getStatus().intValue());

        // drop index
        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testBinaryVectors() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, true, false,
                Collections.singletonList(DataType.BinaryVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.BinaryVector.name())
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withExtraParam("{\"nlist\":64}")
                .withMetricType(MetricType.JACCARD)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR2 = client.createIndex(indexParam2);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR2.getStatus().intValue());

        int rowCount = 10000;
        // insert data by columns
        List<InsertParam.Field> columnsData = generateColumnsData(schema, rowCount, 0);
        R<MutationResult> insertR1 = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(columnsData)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR1.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR1.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids1 = insertResultWrapper.getLongIDs(); // get returned IDs(generated by server-side)
        Assertions.assertEquals(rowCount, ids1.size());

        // Insert entities by rows
        List<JsonObject> rowsData = generateRowsData(schema, rowCount, rowCount);
        R<MutationResult> insertR2 = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rowsData)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR2.getStatus().intValue());

        insertResultWrapper = new MutationResultWrapper(insertR2.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids2 = insertResultWrapper.getLongIDs(); // get returned IDs(generated by server-side)
        Assertions.assertEquals(rowCount, ids2.size());

        // insert test vector, position() is zero with ByteBuffer.wrap()
        byte[] byteArray = new byte[DIMENSION/8];
        for (int i = 0; i < byteArray.length; i++) {
            byteArray[i] = (byte) ((i%3 == 0) ? 255 : 0);
        }
        ByteBuffer testBuffer = ByteBuffer.wrap(byteArray);
        List<InsertParam.Field> testData =
                Collections.singletonList(new InsertParam.Field(DataType.BinaryVector.name(), Collections.singletonList(testBuffer)));
        R<MutationResult> insertR3 = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(testData)
                .build());
        insertResultWrapper = new MutationResultWrapper(insertR3.getData());
        Long testID = insertResultWrapper.getLongIDs().get(0);

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());
        Assertions.assertEquals(2*rowCount+1, stat.getRowCount());

        // check index
        while(true) {
            DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                    .withCollectionName(randomCollectionName)
                    .withFieldName(DataType.BinaryVector.name())
                    .build();
            R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
            Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

            DescIndexResponseWrapper indexDescWrapper = new DescIndexResponseWrapper(descIndexR.getData());
            DescIndexResponseWrapper.IndexDesc indexDesc = indexDescWrapper.getIndexDescByFieldName(DataType.BinaryVector.name());
            Assertions.assertNotNull(indexDesc);
            if (indexDesc.getTotalRows() != indexDesc.getIndexedRows()) {
                System.out.println("Waiting index to be finished...");
                TimeUnit.SECONDS.sleep(1);
                continue;
            }
            Assertions.assertEquals(DataType.BinaryVector.name(), indexDesc.getFieldName());
            Assertions.assertEquals(IndexType.BIN_IVF_FLAT, indexDesc.getIndexType());
            Assertions.assertEquals(MetricType.JACCARD, indexDesc.getMetricType());
            Assertions.assertEquals(2*rowCount+1, indexDesc.getTotalRows());
            Assertions.assertEquals(2*rowCount+1, indexDesc.getIndexedRows());
            Assertions.assertEquals(0L, indexDesc.getPendingIndexRows());
            Assertions.assertTrue(indexDesc.getIndexFailedReason().isEmpty());
            System.out.println("Index description: " + indexDesc);
            break;
        }

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query
        Long fetchID = ids1.get(0);
        ByteBuffer fetchVector = (ByteBuffer)columnsData.get(0).getValues().get(0);
        R<QueryResults> fetchR = client.query(QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("id == %d", fetchID))
                .addOutField(DataType.BinaryVector.name())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), fetchR.getStatus().intValue());
        QueryResultsWrapper fetchWrapper = new QueryResultsWrapper(fetchR.getData());
        FieldDataWrapper fetchField = fetchWrapper.getFieldWrapper(DataType.BinaryVector.name());
        Assertions.assertEquals(1L, fetchField.getRowCount());
        List<?> fetchObj = fetchField.getFieldData();
        Assertions.assertEquals(1, fetchObj.size());
        Assertions.assertInstanceOf(ByteBuffer.class, fetchObj.get(0));
        ByteBuffer fetchBuffer = (ByteBuffer) fetchObj.get(0);
        Assertions.assertArrayEquals(fetchVector.array(), fetchBuffer.array());

        // search with BIN_FLAT index
        int searchTarget = 99;
        ByteBuffer targetVector = (ByteBuffer)columnsData.get(0).getValues().get(searchTarget);

        SearchParam searchOneParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.JACCARD)
                .withTopK(5)
                .withBinaryVectors(Collections.singletonList(targetVector))
                .withVectorFieldName(DataType.BinaryVector.name())
                .addOutField(DataType.BinaryVector.name())
                .build();

        R<SearchResults> searchOne = client.search(searchOneParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchOne.getStatus().intValue());

        SearchResultsWrapper oneResult = new SearchResultsWrapper(searchOne.getData().getResults());
        List<SearchResultsWrapper.IDScore> oneScores = oneResult.getIDScore(0);
        System.out.println("The search result of id " + ids1.get(searchTarget) + " with SUPERSTRUCTURE metric:");
        System.out.println(oneScores);

        // verify the output vector, the top1 item is equal to the target vector
        List<?> items = oneResult.getFieldData(DataType.BinaryVector.name(), 0);
        Assertions.assertEquals(items.size(), 5);
        ByteBuffer firstItem = (ByteBuffer) items.get(0);
        Assertions.assertArrayEquals(targetVector.array(), firstItem.array());

        // release collection
        ReleaseCollectionParam releaseCollectionParam = ReleaseCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> releaseCollectionR = client.releaseCollection(releaseCollectionParam);
        Assertions.assertEquals(R.Status.Success.getCode(), releaseCollectionR.getStatus().intValue());

        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.BinaryVector.name())
                .withIndexName("abv")
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withExtraParam("{\"nlist\":64}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR2 = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR2.getStatus().intValue());

        // pick some vectors to search with index
        int nq = 5;
        List<Long> targetVectorIDs = new ArrayList<>();
        List<ByteBuffer> targetVectors = new ArrayList<>();
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add(ids1.get(i));
            targetVectors.add((ByteBuffer) columnsData.get(0).getValues().get(i));
        }
        targetVectors.add(testBuffer);
        targetVectorIDs.add(testID);

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.HAMMING)
                .withTopK(topK)
                .withBinaryVectors(targetVectors)
                .withVectorFieldName(DataType.BinaryVector.name())
                .withParams("{\"nprobe\":8}")
                .withOutFields(Collections.singletonList(DataType.BinaryVector.name()))
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            Assertions.assertEquals(targetVectorIDs.get(i), scores.get(0).getLongID());
            ByteBuffer buf = (ByteBuffer) scores.get(0).get(DataType.BinaryVector.name());
            Assertions.assertArrayEquals(targetVectors.get(i).array(), buf.array());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testSparseVector() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.SparseFloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        int rowCount = 10000;
        List<InsertParam.Field> columnsData = generateColumnsData(schema, rowCount, 0);

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(columnsData)
                .build();
        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.SparseFloatVector.name())
                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                .withMetricType(MetricType.IP)
                .withExtraParam("{\"drop_ratio_build\":0.2}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query
        Long fetchID = (Long)columnsData.get(0).getValues().get(0);
        SortedMap<Long, Float> fetchVector = (SortedMap<Long, Float>)columnsData.get(1).getValues().get(0);
        R<QueryResults> fetchR = client.query(QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("id == %d", fetchID))
                .addOutField(DataType.SparseFloatVector.name())
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), fetchR.getStatus().intValue());
        QueryResultsWrapper fetchWrapper = new QueryResultsWrapper(fetchR.getData());
        FieldDataWrapper fetchField = fetchWrapper.getFieldWrapper(DataType.SparseFloatVector.name());
        Assertions.assertEquals(1L, fetchField.getRowCount());
        List<?> fetchObj = fetchField.getFieldData();
        Assertions.assertEquals(1, fetchObj.size());
        Assertions.assertInstanceOf(SortedMap.class, fetchObj.get(0));
        SortedMap<Long, Float> fetchSparse = (SortedMap<Long, Float>) fetchObj.get(0);
        Assertions.assertEquals(fetchVector.size(), fetchSparse.size());
        for (Long key : fetchVector.keySet()) {
            Assertions.assertTrue(fetchSparse.containsKey(key));
            Assertions.assertEquals(fetchVector.get(key), fetchSparse.get(key));
        }

        // pick some vectors to search with index
        int nq = 5;
        List<Long> targetVectorIDs = new ArrayList<>();
        List<SortedMap<Long, Float>> targetVectors = new ArrayList<>();
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add((Long)columnsData.get(0).getValues().get(i));
            targetVectors.add((SortedMap<Long, Float>)columnsData.get(1).getValues().get(i));
        }

        System.out.println("Search target IDs:" + targetVectorIDs);
        System.out.println("Search target vectors:" + targetVectors);

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withSparseFloatVectors(targetVectors)
                .withVectorFieldName(DataType.SparseFloatVector.name())
                .addOutField(DataType.SparseFloatVector.name())
                .withParams("{\"drop_ratio_search\":0.2}")
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            if (targetVectorIDs.get(i) != scores.get(0).getLongID()) {
                System.out.println(targetVectors.get(i));
            }
            Assertions.assertEquals(targetVectorIDs.get(i), scores.get(0).getLongID());

            Object v = scores.get(0).get(DataType.SparseFloatVector.name());
            SortedMap<Long, Float> sparse = (SortedMap<Long, Float>)v;
            Assertions.assertEquals(sparse, targetVectors.get(i));
            Assertions.assertEquals(targetVectors.get(i).size(), sparse.size());
            for (Long key : sparse.keySet()) {
                Assertions.assertTrue(targetVectors.get(i).containsKey(key));
                Assertions.assertEquals(sparse.get(key), targetVectors.get(i).get(key));
            }
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testFloat16Utils() {
        List<List<Float>> originVectors = utils.generateFloatVectors(10);

        for (List<Float> originalVector : originVectors) {
            ByteBuffer fp16Buffer = Float16Utils.f32VectorToFp16Buffer(originalVector);
            List<Float> fp16Vec = Float16Utils.fp16BufferToVector(fp16Buffer);
            for (int i = 0; i < originalVector.size(); i++) {
                Assertions.assertEquals(fp16Vec.get(i), originalVector.get(i), 0.01);
            }

            ByteBuffer bf16Buffer = Float16Utils.f32VectorToBf16Buffer(originalVector);
            List<Float> bf16Vec = Float16Utils.bf16BufferToVector(bf16Buffer);
            for (int i = 0; i < originalVector.size(); i++) {
                Assertions.assertEquals(bf16Vec.get(i), originalVector.get(i), 0.1);
            }
        }
    }

    @Test
    void testFloat16Vector() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Arrays.asList(DataType.Float16Vector, DataType.BFloat16Vector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        R<RpcStatus> createIndexR = client.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.Float16Vector.name())
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.COSINE)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        createIndexR = client.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.BFloat16Vector.name())
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.COSINE)
                .withExtraParam("{\"nlist\": 128}")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection(partial load)
        List<String> loadFields = new ArrayList<>();
        loadFields.add("id");
        loadFields.add(DataType.Float16Vector.name());
        loadFields.add(DataType.BFloat16Vector.name());
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withLoadFields(loadFields)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // generate vectors
        int rowCount = 10000;
        List<List<Float>> vectors = utils.generateFloatVectors(rowCount);

        // insert by column-based
        List<ByteBuffer> fp16Vectors = new ArrayList<>();
        List<ByteBuffer> bf16Vectors = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            ids.add((long)i);
            List<Float> vector = vectors.get(i);
            ByteBuffer fp16Vector = Float16Utils.f32VectorToFp16Buffer(vector);
            fp16Vectors.add(fp16Vector);
            ByteBuffer bf16Vector = Float16Utils.f32VectorToBf16Buffer(vector);
            bf16Vectors.add(bf16Vector);
        }

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("id", ids));
        fields.add(new InsertParam.Field(DataType.Float16Vector.name(), fp16Vectors));
        fields.add(new InsertParam.Field(DataType.BFloat16Vector.name(), bf16Vectors));

        R<MutationResult> insertColumnResp = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertColumnResp.getStatus().intValue());
        System.out.println(ids.size() + " rows inserted");

        // insert by row-based
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i + 5000);

            List<Float> vector = vectors.get(i + 5000);
            ByteBuffer fp16Vector = Float16Utils.f32VectorToFp16Buffer(vector);
            row.add(DataType.Float16Vector.name(), JsonUtils.toJsonTree(fp16Vector.array()));
            ByteBuffer bf16Vector = Float16Utils.f32VectorToBf16Buffer(vector);
            row.add(DataType.BFloat16Vector.name(), JsonUtils.toJsonTree(bf16Vector.array()));
            rows.add(row);
        }

        insertColumnResp = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertColumnResp.getStatus().intValue());
        System.out.println(rows.size() + " rows inserted");

        // query
        List<Long> targetIDs = Arrays.asList(100L, 8888L);
        String expr = String.format("id in %s", targetIDs);
        R<QueryResults> fetchR = client.query(QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .addOutField(DataType.Float16Vector.name())
                .addOutField(DataType.BFloat16Vector.name())
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), fetchR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper fetchWrapper = new QueryResultsWrapper(fetchR.getData());
        List<QueryResultsWrapper.RowRecord> records = fetchWrapper.getRowRecords();
        Assertions.assertEquals(targetIDs.size(), records.size());
        for (int i = 0; i < records.size(); i++) {
            QueryResultsWrapper.RowRecord record = records.get(i);
            Assertions.assertEquals(targetIDs.get(i), record.get("id"));
            Assertions.assertInstanceOf(ByteBuffer.class, record.get(DataType.Float16Vector.name()));
            Assertions.assertInstanceOf(ByteBuffer.class, record.get(DataType.BFloat16Vector.name()));

            List<Float> originVector = vectors.get(targetIDs.get(i).intValue());
            ByteBuffer buf1 = (ByteBuffer) record.get(DataType.Float16Vector.name());
            List<Float> fp16Vec = Float16Utils.fp16BufferToVector(buf1);
            Assertions.assertEquals(fp16Vec.size(), originVector.size());
            for (int k = 0; k < fp16Vec.size(); k++) {
                Assertions.assertTrue(Math.abs(fp16Vec.get(k) - originVector.get(k)) <= FLOAT16_PRECISION);
            }

            ByteBuffer buf2 = (ByteBuffer) record.get(DataType.BFloat16Vector.name());
            List<Float> bf16Vec = Float16Utils.bf16BufferToVector(buf2);
            Assertions.assertEquals(bf16Vec.size(), originVector.size());
            for (int k = 0; k < bf16Vec.size(); k++) {
                Assertions.assertTrue(Math.abs(bf16Vec.get(k) - originVector.get(k)) <= BFLOAT16_PRECISION);
            }
        }

        // search float16 vector
        long targetID = new Random().nextInt(rowCount);
        List<Float> originVector = vectors.get((int) targetID);
        ByteBuffer fp16Vector = Float16Utils.f32VectorToFp16Buffer(originVector);

        int topK = 5;
        R<SearchResults> searchR = client.search(SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(topK)
                .withFloat16Vectors(Collections.singletonList(fp16Vector))
                .withVectorFieldName(DataType.Float16Vector.name())
                .addOutField(DataType.Float16Vector.name())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result of float16
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        System.out.println("The result of float16 vector(ID = " + targetID + "):");
        System.out.println(scores);
        Assertions.assertEquals(topK, scores.size());
        Assertions.assertEquals(targetID, scores.get(0).getLongID());

        Object v = scores.get(0).get(DataType.Float16Vector.name());
        Assertions.assertInstanceOf(ByteBuffer.class, v);
        List<Float> fp16Vec = Float16Utils.fp16BufferToVector((ByteBuffer)v);
        Assertions.assertEquals(fp16Vec.size(), originVector.size());
        for (int k = 0; k < fp16Vec.size(); k++) {
            Assertions.assertTrue(Math.abs(fp16Vec.get(k) - originVector.get(k)) <= FLOAT16_PRECISION);
        }

        // search bfloat16 vector
        ByteBuffer bf16Vector = Float16Utils.f32VectorToBf16Buffer(vectors.get((int) targetID));
        searchR = client.search(SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(topK)
                .withParams("{\"nprobe\": 16}")
                .withBFloat16Vectors(Collections.singletonList(bf16Vector))
                .withVectorFieldName(DataType.BFloat16Vector.name())
                .addOutField(DataType.BFloat16Vector.name())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result of bfloat16
        results = new SearchResultsWrapper(searchR.getData().getResults());
        scores = results.getIDScore(0);
        System.out.println("The result of bfloat16 vector(ID = " + targetID + "):");
        System.out.println(scores);
        Assertions.assertEquals(topK, scores.size());
        Assertions.assertEquals(targetID, scores.get(0).getLongID());

        v = scores.get(0).get(DataType.BFloat16Vector.name());
        Assertions.assertInstanceOf(ByteBuffer.class, v);
        List<Float> bf16Vec = Float16Utils.bf16BufferToVector((ByteBuffer)v);
        Assertions.assertEquals(bf16Vec.size(), originVector.size());
        for (int k = 0; k < bf16Vec.size(); k++) {
            Assertions.assertTrue(Math.abs(bf16Vec.get(k) - originVector.get(k)) <= BFLOAT16_PRECISION);
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testMultipleVectorFields() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, true, false,
                Arrays.asList(DataType.FloatVector, DataType.BinaryVector, DataType.SparseFloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data to multiple vector fields
        int rowCount = 10000;
        List<InsertParam.Field> fields = generateColumnsData(schema, rowCount, 0);

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // create indexes on multiple vector fields
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.COSINE)
                .withExtraParam("{\"nlist\":64}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.BinaryVector.name())
                .withIndexType(IndexType.BIN_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withExtraParam("{}")
                .build();

        createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.SparseFloatVector.name())
                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                .withMetricType(MetricType.IP)
                .withExtraParam("{\"drop_ratio_build\":0.2}")
                .build();

        createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // search on multiple vector fields
        AnnSearchParam param1 = AnnSearchParam.newBuilder()
                .withVectorFieldName(DataType.FloatVector.name())
                .withFloatVectors(utils.generateFloatVectors(1))
                .withMetricType(MetricType.COSINE)
                .withParams("{\"nprobe\": 32}")
                .withTopK(10)
                .build();

        AnnSearchParam param2 = AnnSearchParam.newBuilder()
                .withVectorFieldName(DataType.BinaryVector.name())
                .withBinaryVectors(utils.generateBinaryVectors(1))
                .withMetricType(MetricType.HAMMING)
                .withParams("{}")
                .withTopK(5)
                .build();

        AnnSearchParam param3 = AnnSearchParam.newBuilder()
                .withVectorFieldName(DataType.SparseFloatVector.name())
                .withSparseFloatVectors(utils.generateSparseVectors(1))
                .withMetricType(MetricType.IP)
                .withParams("{\"drop_ratio_search\":0.2}")
                .withTopK(7)
                .build();

        HybridSearchParam searchParam = HybridSearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addOutField(DataType.SparseFloatVector.name())
                .addSearchRequest(param1)
                .addSearchRequest(param2)
                .addSearchRequest(param3)
                .withTopK(3)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withRanker(WeightedRanker.newBuilder()
                        .withWeights(Lists.newArrayList(0.5f, 0.5f, 1.0f))
                        .build())
                .withOutFields(Collections.singletonList("*"))
                .build();

        R<SearchResults> searchR = client.hybridSearch(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // print search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        for (SearchResultsWrapper.IDScore score : scores) {
            System.out.println(score);
            Object id = score.get("id");
            Assertions.assertInstanceOf(Long.class, id);
            Object fv = score.get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, fv);
            List<Float> fvec = (List<Float>)fv;
            Assertions.assertEquals(DIMENSION, fvec.size());
            Object bv = score.get(DataType.BinaryVector.name());
            Assertions.assertInstanceOf(ByteBuffer.class, bv);
            ByteBuffer bvec = (ByteBuffer)bv;
            Assertions.assertEquals(DIMENSION, bvec.limit()*8);
            Object sv = score.get(DataType.SparseFloatVector.name());
            Assertions.assertInstanceOf(SortedMap.class, sv);
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testAsyncMethods() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, true, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert async
        List<ListenableFuture<R<MutationResult>>> futureResponses = new ArrayList<>();
        int rowCount = 1000;
        for (long i = 0L; i < 10; ++i) {
            List<List<Float>> vectors = utils.generateFloatVectors(rowCount);
            List<InsertParam.Field> fieldsInsert = new ArrayList<>();
            fieldsInsert.add(new InsertParam.Field(DataType.FloatVector.name(), vectors));

            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(randomCollectionName)
                    .withFields(fieldsInsert)
                    .build();

            ListenableFuture<R<MutationResult>> insertFuture = client.insertAsync(insertParam);
            futureResponses.add(insertFuture);
        }

        // get insert result
        List<Long> queryIDs = new ArrayList<>();
        for (ListenableFuture<R<MutationResult>> response : futureResponses) {
            try {
                R<MutationResult> insertR = response.get();
                Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

                MutationResultWrapper wrapper = new MutationResultWrapper(insertR.getData());
                queryIDs.add(wrapper.getLongIDs().get(0));
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("failed to insert:" + e.getMessage());
                return;
            }
        }

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.IP)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // search async
        List<List<Float>> targetVectors = utils.generateFloatVectors(2);
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .build();

        ListenableFuture<R<SearchResults>> searchFuture = client.searchAsync(searchParam);

        // query async
        String expr = "id in " + queryIDs;
        List<String> outputFields = Arrays.asList("id", DataType.FloatVector.name());
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        ListenableFuture<R<QueryResults>> queryFuture = client.queryAsync(queryParam);

        try {
            // get search results
            R<SearchResults> searchR = searchFuture.get();
            Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

            // verify search result
            SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
            System.out.println("Search results:");
            for (int i = 0; i < targetVectors.size(); ++i) {
                List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
                Assertions.assertEquals(topK, scores.size());
                System.out.println(scores.toString());
            }

            // get query results
            R<QueryResults> queryR = queryFuture.get();
            Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

            // verify query result
            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
            for (String fieldName : outputFields) {
                FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
                System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
                System.out.println(wrapper.getFieldData());
                Assertions.assertEquals(queryIDs.size(), wrapper.getFieldData().size());
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    // this case can be executed when the milvus image of version 2.1 is published.
    @Test
    void testCredential() {
        String randomCollectionName = generator.generate(10);
        // collection schema
        CollectionSchemaParam schema = buildSchema(false, true, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.IVF_FLAT)
                .withIndexName("xxx")
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":256}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        client.getIndexState(GetIndexStateParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());

        R<RpcStatus> dropIndexR = client.dropIndex(DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testStringField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(true, false, false,
                Arrays.asList(DataType.FloatVector, DataType.VarChar, DataType.Int64));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        // insert data
        int rowCount = 10000;
        List<InsertParam.Field> columnsData = generateColumnsData(schema, rowCount, 0);
        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(columnsData)
                .build();

        R<MutationResult> insertR = client.withTimeout(10, TimeUnit.SECONDS).insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.VarChar.name())
                .withIndexName("stridx")
                .withIndexType(IndexType.TRIE)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.VarChar.name())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.IP)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR2 = client.createIndex(indexParam2);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR2.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query vectors to verify
        List<Long> queryItems = new ArrayList<>();
        List<String> queryIds = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            queryIds.add((String)columnsData.get(0).getValues().get(i));
            queryItems.add((Long)columnsData.get(3).getValues().get(i));
        }
        String expr = DataType.Int64.name() + " in " + queryItems;
        List<String> outputFields = Arrays.asList("id", DataType.VarChar.name());
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
            Assertions.assertEquals(nq, wrapper.getFieldData().size());

            if (fieldName.compareTo("id") == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper("id").getFieldData();
                Assertions.assertEquals(nq, out.size());
                for (Object o : out) {
                    String id = (String) o;
                    Assertions.assertTrue(queryIds.contains(id));
                }
            }
        }

        // search
        int topK = 5;
        List<List<Float>> targetVectors = new ArrayList<>();
        for (Long seq : queryItems) {
            targetVectors.add((List<Float>)columnsData.get(1).getValues().get(seq.intValue()));
        }
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withFloatVectors(targetVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .addOutField(DataType.Int64.name())
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + queryIds.get(i) + "):");
            System.out.println(scores);
            Assertions.assertEquals(scores.get(0).getStrID(), queryIds.get(i));
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    private static void testIndex(String collectionName, String fieldName,
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

    @Test
    void testFloatVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        Map<IndexType, String> indexTypes = new HashMap<>();
        indexTypes.put(IndexType.FLAT, "{}");
        indexTypes.put(IndexType.IVF_FLAT, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_SQ8, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_PQ, "{\"nlist\":128, \"m\":16, \"nbits\":8}");
        indexTypes.put(IndexType.HNSW, "{\"M\":16,\"efConstruction\":64}");

        List<MetricType> metricTypes = new ArrayList<>();
        metricTypes.add(MetricType.L2);
        metricTypes.add(MetricType.IP);

        for (IndexType type : indexTypes.keySet()) {
            for (MetricType metric : metricTypes) {
                testIndex(randomCollectionName, DataType.FloatVector.name(), type, metric, indexTypes.get(type), Boolean.TRUE);
                testIndex(randomCollectionName, DataType.FloatVector.name(), type, metric, indexTypes.get(type), Boolean.FALSE);
            }
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testBinaryVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.BinaryVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        List<MetricType> flatMetricTypes = new ArrayList<>();
        flatMetricTypes.add(MetricType.HAMMING);
        flatMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : flatMetricTypes) {
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_FLAT, metric, "{}", Boolean.TRUE);
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_FLAT, metric, "{}", Boolean.FALSE);
        }

        List<MetricType> ivfMetricTypes = new ArrayList<>();
        ivfMetricTypes.add(MetricType.HAMMING);
        ivfMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : ivfMetricTypes) {
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.TRUE);
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.FALSE);
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testDynamicField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, true,
                Arrays.asList(DataType.FloatVector, DataType.JSON));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexName("abv")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.COSINE)
                .withExtraParam("{}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        int rowCount = 10;
        // insert data by row-based
        List<JsonObject> rowsData = generateRowsData(schema, rowCount, 0);
        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rowsData)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // insert data by column-based
        List<InsertParam.Field> columnsData = generateColumnsData(schema, rowCount, rowCount);
        InsertParam insertColumnsParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(columnsData)
                .build();

        R<MutationResult> insertColumnResp = client.insert(insertColumnsParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertColumnResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // retrieve rows
        List<Long> target = Arrays.asList(0L, 5L, 9L, 16L, 19L);
        String expr = "dynamic in " + target;
        List<String> outputFields = Arrays.asList(DataType.JSON.name(), "dynamic");
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results with expr: " + expr);
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object extraMeta = record.get("dynamic");
            Assertions.assertInstanceOf(Long.class, extraMeta);
            Assertions.assertTrue(target.contains(extraMeta));
            System.out.println("'dynamic' is from dynamic field, value: " + extraMeta);
        }

        // search the No.11 and No.15
        target = Arrays.asList(1L, 5L);
        List<List<Float>> targetVectors = new ArrayList<>();
        targetVectors.add((List<Float>)columnsData.get(1).getValues().get(target.get(0).intValue()));
        targetVectors.add((List<Float>)columnsData.get(1).getValues().get(target.get(1).intValue()));
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(topK)
                .withFloatVectors(targetVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .withParams("{}")
                .withOutFields(outputFields)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector:");
            SearchResultsWrapper.IDScore score = scores.get(0);
            System.out.println(score);
            Object extraMeta = score.get("dynamic");
            Assertions.assertInstanceOf(Long.class, extraMeta);
            Long k = (Long)extraMeta - rowCount;
            Assertions.assertTrue(target.contains(k));
            System.out.println("'dynamic' is from dynamic field, value: " + extraMeta);
        }
        Assertions.assertEquals(results.getIDScore(0).get(0).getLongID(), 11L);
        Assertions.assertEquals(results.getIDScore(1).get(0).getLongID(), 15L);

        // retrieve dynamic values inserted by column-based
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("dynamic == 18")
                .withOutFields(Collections.singletonList("*"))
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results with expr: " + expr);
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            long id = (long)record.get("id");
            Assertions.assertEquals(18L, id);
            Object vec = record.get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, vec);
            List<Float> vector = (List<Float>)vec;
            Assertions.assertEquals(DIMENSION, vector.size());
            Object j = record.get(DataType.JSON.name());
            Assertions.assertInstanceOf(JsonObject.class, j);
            JsonObject jon = (JsonObject)j;
            Assertions.assertTrue(jon.has("json"));
        }

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testArrayField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Arrays.asList(DataType.FloatVector, DataType.Array));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
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

        String varcharArrayName = DataType.Array + "_varchar";
        String intArrayName = DataType.Array + "_int32";
        String floatArrayName = DataType.Array + "_float";

        // insert data by column-based
        int rowCount = 100;
        List<Long> ids = new ArrayList<>();
        List<List<String>> strArrArray = new ArrayList<>();
        List<List<Integer>> intArrArray = new ArrayList<>();
        List<List<Float>> floatArrArray = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            ids.add((long)i);
            List<String> strArray = new ArrayList<>();
            List<Integer> intArray = new ArrayList<>();
            List<Float> floatArray = new ArrayList<>();
            for (int k = 0; k < i; k++) {
                strArray.add(String.format("C_StringArray_%d_%d", i, k));
                intArray.add(i*10000 + k);
                floatArray.add((float)k/1000 + i);
            }
            strArrArray.add(strArray);
            intArrArray.add(intArray);
            floatArrArray.add(floatArray);
        }
        List<List<Float>> vectors = utils.generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field("id", ids));
        fieldsInsert.add(new InsertParam.Field(DataType.FloatVector.name(), vectors));
        fieldsInsert.add(new InsertParam.Field(varcharArrayName, strArrArray));
        fieldsInsert.add(new InsertParam.Field(intArrayName, intArrArray));
        fieldsInsert.add(new InsertParam.Field(floatArrayName, floatArrArray));

        InsertParam insertColumnsParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertColumnResp = client.insert(insertColumnsParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertColumnResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // insert data by row-based
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 10000L + (long)i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));

            List<String> strArray = new ArrayList<>();
            List<Integer> intArray = new ArrayList<>();
            List<Float> floatArray = new ArrayList<>();
            for (int k = 0; k < i; k++) {
                strArray.add(String.format("R_StringArray_%d_%d", i, k));
                intArray.add(i*10000 + k);
                floatArray.add((float)k/1000 + i);
            }
            row.add(varcharArrayName, JsonUtils.toJsonTree(strArray));
            row.add(intArrayName, JsonUtils.toJsonTree(intArray));
            row.add(floatArrayName, JsonUtils.toJsonTree(floatArray));

            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // search
        List<List<Float>> searchVectors = utils.generateFloatVectors(1);
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(5)
                .withFloatVectors(searchVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .addOutField(varcharArrayName)
                .addOutField(intArrayName)
                .addOutField(floatArrayName)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        System.out.println("Search results:");
        for (SearchResultsWrapper.IDScore score : scores) {
            System.out.println(score);
            long id = score.getLongID();
            List<?> strArray = (List<?>)score.get(varcharArrayName);
            Assertions.assertEquals(id%10000, (long)strArray.size());
            List<?> intArray = (List<?>)score.get(intArrayName);
            Assertions.assertEquals(id%10000, (long)intArray.size());
            List<?> floatArray = (List<?>)score.get(floatArrayName);
            Assertions.assertEquals(id%10000, (long)floatArray.size());
        }

        // search with array_contains
        searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withExpr(String.format("array_contains_any(%s, [450038, 680015])", intArrayName))
                .withFloatVectors(searchVectors)
                .withVectorFieldName(DataType.FloatVector.name())
                .addOutField(varcharArrayName)
                .addOutField(intArrayName)
                .addOutField(floatArrayName)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());
        results = new SearchResultsWrapper(searchR.getData().getResults());
        scores = results.getIDScore(0);
        System.out.println("Search results:");
        for (SearchResultsWrapper.IDScore score : scores) {
            System.out.println(score);
            long id = score.getLongID();
            Assertions.assertTrue(id == 10068 || id == 68 || id == 10045 || id == 45);
        }

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testUpsert() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, true,
                Arrays.asList(DataType.FloatVector, DataType.VarChar));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data by row-based with id from 0 ~ 9
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
            row.addProperty(DataType.VarChar.name(), String.format("name_%d", i));
            row.addProperty("dynamic_value", String.format("dynamic_%d", i));
            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // get collection statistics with flush, the 10 rows are flushed to a sealed segment
        // wait 2 seconds, ensure the data node consumes the data
        TimeUnit.SECONDS.sleep(2);
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
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

        // retrieve one row from the sealed segment
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 5")
                .addOutField(DataType.VarChar.name())
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in sealed segment:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object name = record.get(DataType.VarChar.name());
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_5", name);
        }

        // insert 10 rows into growing segment with id from 10 ~ 19
        // since the ids are not exist, the upsert call is equal to an insert call
        rows.clear();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", rowCount + i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
            row.addProperty(DataType.VarChar.name(), String.format("name_%d", rowCount + i));
            rows.add(row);
        }

        UpsertParam upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // retrieve one row from the growing segment
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 18")
                .addOutField(DataType.VarChar.name())
                .addOutField("dynamic_value")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in growing segment:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object name = record.get(DataType.VarChar.name());
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_18", name);
            Assertions.assertFalse(record.contains("dynamic_value"));
            Assertions.assertNull(record.get("dynamic_value")); // we didn't set dynamic_value for No.18 row
        }

        // upsert to change the no.5 and no.18 items
        rows.clear();
        JsonObject row = new JsonObject();
        row.addProperty("id", 5L);
        List<Float> vector = utils.generateFloatVectors(1).get(0);
        row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
        row.addProperty(DataType.VarChar.name(), "updated_5");
        row.addProperty("dynamic_value", String.format("dynamic_%d", 5));
        rows.add(row);
        row = new JsonObject();
        row.addProperty("id", 18L);
        vector = utils.generateFloatVectors(1).get(0);
        row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
        row.addProperty(DataType.VarChar.name(), "updated_18");
        row.addProperty("dynamic_value", 18);
        rows.add(row);

        upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());

        // verify the two items
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 5 || id == 18")
                .addOutField(DataType.VarChar.name())
                .addOutField("dynamic_value")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("updated_5", records.get(0).get(DataType.VarChar.name()));
        Assertions.assertEquals("dynamic_5", records.get(0).get("dynamic_value"));
        Assertions.assertEquals("updated_18", records.get(1).get(DataType.VarChar.name()));
        Assertions.assertEquals(18L, records.get(1).get("dynamic_value"));

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testAlias() {
        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection A
        R<RpcStatus> response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_A")
                .withSchema(schema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        // create collection B
        response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_B")
                .withSchema(schema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        // create alias
        response = client.createAlias(CreateAliasParam.newBuilder()
                .withCollectionName("coll_A")
                .withAlias("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        R<Boolean> has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), true);

        R<ListAliasesResponse> listResp = client.listAliases(ListAliasesParam.newBuilder()
                .withCollectionName("coll_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());
        Assertions.assertEquals(listResp.getData().getAliases(0), "alias_A");
        Assertions.assertEquals(listResp.getData().getCollectionName(), "coll_A");

        // alter alias
        response = client.alterAlias(AlterAliasParam.newBuilder()
                .withAlias("alias_A")
                .withCollectionName("coll_B")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());

        has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), true);

        listResp = client.listAliases(ListAliasesParam.newBuilder()
                .withCollectionName("coll_B")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());
        Assertions.assertEquals(listResp.getData().getAliases(0), "alias_A");
        Assertions.assertEquals(listResp.getData().getCollectionName(), "coll_B");

        // drop alias
        response = client.dropAlias(DropAliasParam.newBuilder()
                .withAlias("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), false);
    }

    @Test
    void testHighLevelGet() {
        // collection schema
        String field1Name = "id_field";
        String field2Name = "vector_field";
        FieldType int64PrimaryField = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build();

        FieldType varcharPrimaryField = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withDataType(DataType.VarChar)
                .withName(field1Name)
                .withMaxLength(128)
                .build();

        FieldType vectorField = FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(DIMENSION)
                .build();

        testCollectionHighLevelGet(int64PrimaryField, vectorField);
        testCollectionHighLevelGet(varcharPrimaryField, vectorField);
    }

//    @Test
//    void testHighLevelDelete() {
//        // collection schema
//        String field1Name = "id_field";
//        String field2Name = "vector_field";
//        FieldType int64PrimaryField = FieldType.newBuilder()
//                .withPrimaryKey(true)
//                .withAutoID(false)
//                .withDataType(DataType.Int64)
//                .withName(field1Name)
//                .build();
//
//        FieldType varcharPrimaryField = FieldType.newBuilder()
//                .withPrimaryKey(true)
//                .withDataType(DataType.VarChar)
//                .withName(field1Name)
//                .withMaxLength(128)
//                .build();
//
//        FieldType vectorField = FieldType.newBuilder()
//                .withDataType(DataType.FloatVector)
//                .withName(field2Name)
//                .withDimension(DIMENSION)
//                .build();
//
//        testCollectionHighLevelDelete(int64PrimaryField, vectorField);
//        testCollectionHighLevelDelete(varcharPrimaryField, vectorField);
//    }

    void testCollectionHighLevelGet(FieldType primaryField, FieldType vectorField) {
        // create collection
        String randomCollectionName = generator.generate(10);
        highLevelCreateCollection(primaryField, vectorField, randomCollectionName);

        // insert data
        List<String> primaryIds = new ArrayList<>();
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            if (primaryField.getDataType() == DataType.Int64) {
                row.addProperty(primaryField.getName(), i);
            } else {
                row.addProperty(primaryField.getName(), String.valueOf(i));
            }
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), JsonUtils.toJsonTree(vector));
            rows.add(row);
            primaryIds.add(String.valueOf(i));
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());

        testHighLevelGet(randomCollectionName, primaryIds);
        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    private static void highLevelCreateCollection(FieldType primaryField, FieldType vectorField, String randomCollectionName) {
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

    void testCollectionHighLevelDelete(FieldType primaryField, FieldType vectorField) {
        // create collection & buildIndex & loadCollection
        String randomCollectionName = generator.generate(10);
        highLevelCreateCollection(primaryField, vectorField, randomCollectionName);

        // insert data
        List<String> primaryIds = new ArrayList<>();
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            if (primaryField.getDataType() == DataType.Int64) {
                row.addProperty(primaryField.getName(), i);
            } else {
                row.addProperty(primaryField.getName(), String.valueOf(i));
            }
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), JsonUtils.toJsonTree(vector));
            rows.add(row);
            primaryIds.add(String.valueOf(i));
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());

        // high level delete
        testHighLevelDelete(randomCollectionName, primaryIds);
        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    private static void testHighLevelGet(String collectionName, List primaryIds) {
        GetIdsParam getIdsParam = GetIdsParam.newBuilder()
                .withCollectionName(collectionName)
                .withPrimaryIds(primaryIds)
                .build();

        R<GetResponse> getResponseR = client.get(getIdsParam);
        String outPutStr = String.format("collectionName:%s, primaryIds:%s, getResponseR:%s", collectionName, primaryIds, getResponseR.getData());
        System.out.println(outPutStr);
        Assertions.assertEquals(R.Status.Success.getCode(), getResponseR.getStatus().intValue());
    }

    private static void testHighLevelDelete(String collectionName, List primaryIds) {
        DeleteIdsParam deleteIdsParam = DeleteIdsParam.newBuilder()
                .withCollectionName(collectionName)
                .withPrimaryIds(primaryIds)
                .build();

        R<DeleteResponse> deleteResponseR = client.delete(deleteIdsParam);
        String outPutStr = String.format("collectionName:%s, primaryIds:%s, deleteResponseR:%s", collectionName, primaryIds, deleteResponseR);
        System.out.println(outPutStr);
        Assertions.assertEquals(R.Status.Success.getCode(), deleteResponseR.getStatus().intValue());
    }

    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);

        CollectionSchemaParam schema = buildSchema(true, false, true,
                Arrays.asList(DataType.FloatVector, DataType.JSON));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data
        int rowCount = 1000;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", Long.toString(i));
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(utils.generateFloatVectors(1).get(0)));
            JsonObject json = new JsonObject();
            if (i%2 == 0) {
                json.addProperty("even", true);
            }
            row.add(DataType.JSON.name(), json);
            row.addProperty("dynamic", i);
            rows.add(row);
        }

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query iterator
        QueryIteratorParam.Builder queryIteratorParamBuilder = QueryIteratorParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("dynamic < 300")
                .withOutFields(Lists.newArrayList("*"))
                .withBatchSize(100L)
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED);

        R<QueryIterator> qResponse = client.queryIterator(queryIteratorParamBuilder.build());
        Assertions.assertEquals(R.Status.Success.getCode(), qResponse.getStatus().intValue());

        QueryIterator queryIterator = qResponse.getData();
        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Long.class, record.get("dynamic"));
                Assertions.assertInstanceOf(String.class, record.get("id"));
                Object vec = record.get(DataType.FloatVector.name());
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>)vec;
                Assertions.assertEquals(DIMENSION, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(DataType.JSON.name()));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(300, counter);

        // search iterator
        List<List<Float>> vectors = utils.generateFloatVectors(1);
        SearchIteratorParam.Builder searchIteratorParamBuilder = SearchIteratorParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withOutFields(Lists.newArrayList("*"))
                .withBatchSize(10L)
                .withVectorFieldName(DataType.FloatVector.name())
                .withFloatVectors(vectors)
                .withTopK(50)
                .withMetricType(MetricType.L2);

        R<SearchIterator> sResponse = client.searchIterator(searchIteratorParamBuilder.build());
        Assertions.assertEquals(R.Status.Success.getCode(), sResponse.getStatus().intValue());

        SearchIterator searchIterator = sResponse.getData();
        counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Long.class, record.get("dynamic"));
                Assertions.assertInstanceOf(String.class, record.get("id"));
                Object vec = record.get(DataType.FloatVector.name());
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>)vec;
                Assertions.assertEquals(DIMENSION, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(DataType.JSON.name()));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(50, counter);
    }

    @Test
    void testDatabase() {
        String dbName = "test_database";
        CreateDatabaseParam createDatabaseParam = CreateDatabaseParam.newBuilder().withDatabaseName(dbName).withReplicaNumber(1).withResourceGroups(Arrays.asList("rg1")).build();
        R<RpcStatus> createResponse = client.createDatabase(createDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createResponse.getStatus().intValue());

        // check database props
        DescribeDatabaseParam describeDBParam = DescribeDatabaseParam.newBuilder().withDatabaseName(dbName).build();
        R<DescribeDatabaseResponse> describeResponse = client.describeDatabase(describeDBParam);
        Assertions.assertEquals(R.Status.Success.getCode(), describeResponse.getStatus().intValue());
        DescDBResponseWrapper describeDBWrapper = new DescDBResponseWrapper(describeResponse.getData());
        Assertions.assertEquals(dbName, describeDBWrapper.getDatabaseName());
        Assertions.assertEquals(1, describeDBWrapper.getReplicaNumber());
        Assertions.assertEquals(1, describeDBWrapper.getResourceGroups().size());

        // alter database props
        AlterDatabaseParam alterDatabaseParam = AlterDatabaseParam.newBuilder().withDatabaseName(dbName).withReplicaNumber(3).WithResourceGroups(Arrays.asList("rg1", "rg2", "rg3")).build();
        R<RpcStatus> alterDatabaseResponse = client.alterDatabase(alterDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), alterDatabaseResponse.getStatus().intValue());

        // check database props
        describeResponse = client.describeDatabase(describeDBParam);
        Assertions.assertEquals(R.Status.Success.getCode(), describeResponse.getStatus().intValue());
        describeDBWrapper = new DescDBResponseWrapper(describeResponse.getData());
        Assertions.assertEquals(dbName, describeDBWrapper.getDatabaseName());
        Assertions.assertEquals(3, describeDBWrapper.getReplicaNumber());
        Assertions.assertEquals(3, describeDBWrapper.getResourceGroups().size());


        DropDatabaseParam dropDatabaseParam = DropDatabaseParam.newBuilder().withDatabaseName(dbName).build();
        R<RpcStatus> dropResponse = client.dropDatabase(dropDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropResponse.getStatus().intValue());
    }

    @Test
    void testCacheCollectionSchema() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName("id")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(DIMENSION)
                .build());

        // create collection
        R<RpcStatus> createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert
        JsonObject row = new JsonObject();
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVectors(1).get(0)));
        R<MutationResult> insertR = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // drop collection
        client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        // create a new collection with the same name, different schema
        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName("title")
                .withMaxLength(100)
                .build());

        createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert wrong data
        insertR = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertNotEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // insert correct data
        row.addProperty("title", "hello world");
        insertR = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
    }

    @Test
    void testClientPool() {
        try {
            ConnectParam connectParam = ConnectParam.newBuilder()
                    .withUri(milvus.getEndpoint())
                    .build();
            PoolConfig poolConfig = PoolConfig.builder()
                    .build();
            MilvusClientV1Pool pool = new MilvusClientV1Pool(poolConfig, connectParam);

            List<Thread> threadList = new ArrayList<>();
            int threadCount = 10;
            int requestPerThread = 10;
            String key = "192.168.1.1";
            for (int k = 0; k < threadCount; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < requestPerThread; i++) {
                        MilvusClient client = pool.getClient(key);
                        R<GetVersionResponse> resp = client.getVersion();
//                            System.out.printf("%d, %s%n", i, resp.getData().getVersion());
                        System.out.printf("idle %d, active %d%n", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
                        pool.returnClient(key, client);
                    }
                    System.out.println(String.format("Thread %s finished", Thread.currentThread().getName()));
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }

            System.out.println(String.format("idle %d, active %d", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key)));
            pool.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testNullableAndDefaultValue() {
        String randomCollectionName = generator.generate(10);

        CollectionSchemaParam.Builder builder = CollectionSchemaParam.newBuilder();
        builder.addFieldType(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName("id")
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(DIMENSION)
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.Int32)
                .withName("flag")
                .withMaxLength(100)
                .withDefaultValue(10)
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName("desc")
                .withMaxLength(100)
                .withNullable(true)
                .build());
        R<RpcStatus> createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(builder.build())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index on scalar field
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName("vector")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        // insert by row-based
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            List<Float> vector = utils.generateFloatVector();
            row.addProperty("id", i);
            row.add("vector", JsonUtils.toJsonTree(vector));
            if (i%2 == 0) {
                row.addProperty("flag", i);
                row.add("desc", JsonNull.INSTANCE);
            } else {
                row.addProperty("desc", "AAA");
            }
            data.add(row);
        }

        R<MutationResult> insertR = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(data)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // insert by column-based
        List<List<Float>> vectors = utils.generateFloatVectors(10);
        List<Long> ids = new ArrayList<>();
        List<Integer> flags = new ArrayList<>();
        List<String> descs = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            ids.add((long)i);
            if (i%2 == 0) {
                flags.add(i);
                descs.add(null);
            } else {
                flags.add(null);
                descs.add("AAA");
            }

        }
        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field("id", ids));
        fieldsInsert.add(new InsertParam.Field("vector", vectors));
        fieldsInsert.add(new InsertParam.Field("flag", flags));
        fieldsInsert.add(new InsertParam.Field("desc", descs));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // query
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id >= 0")
                .addOutField("flag")
                .addOutField("desc")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results:");
        for (QueryResultsWrapper.RowRecord record:records) {
            long id = (long)record.get("id");
            if (id%2 == 0) {
                Assertions.assertEquals((int)id, record.get("flag"));
                Assertions.assertNull(record.get("desc"));
            } else {
                Assertions.assertEquals(10, record.get("flag"));
                Assertions.assertEquals("AAA", record.get("desc"));
            }
            System.out.println(record);
        }

        // search the row-based items
        List<List<Float>> searchVectors = utils.generateFloatVectors(1);
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withFloatVectors(searchVectors)
                .withVectorFieldName("vector")
                .withParams("{}")
                .addOutField("flag")
                .addOutField("desc")
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        System.out.println("Search results:");
        Assertions.assertEquals(10, scores.size());
        for (SearchResultsWrapper.IDScore score : scores) {
            long id = score.getLongID();
            Map<String, Object> fieldValues = score.getFieldValues();
            if (id%2 == 0) {
                Assertions.assertEquals((int)id, fieldValues.get("flag"));
                Assertions.assertNull(fieldValues.get("desc"));
            } else {
                Assertions.assertEquals(10, fieldValues.get("flag"));
                Assertions.assertEquals("AAA", fieldValues.get("desc"));
            }
            System.out.println(score);
        }
    }
}
