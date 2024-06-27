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
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ParquetReaderUtils;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
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
import io.milvus.response.*;
import org.apache.avro.generic.GenericData;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger logger = LogManager.getLogger("MilvusClientTest");
    protected static MilvusClient client;
    protected static RandomStringGenerator generator;
    protected static final int dimension = 128;

    protected static final Gson GSON_INSTANCE = new Gson();

    @Container
    private static final MilvusContainer milvus = new MilvusContainer("milvusdb/milvus:v2.3.13");

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

    protected List<List<Float>> generateFloatVectors(int count) {
        Random ran = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            List<Float> vector = new ArrayList<>();
            for (int i = 0; i < dimension; ++i) {
                vector.add(ran.nextFloat());
            }
            vectors.add(vector);
        }

        return vectors;
    }

    protected List<List<Float>> normalizeFloatVectors(List<List<Float>> src) {
        for (List<Float> vector : src) {
            double total = 0.0;
            for (Float val : vector) {
                total = total + val * val;
            }
            float squre = (float) Math.sqrt(total);
            for (int i = 0; i < vector.size(); ++i) {
                vector.set(i, vector.get(i) / squre);
            }
        }

        return src;
    }

    protected List<ByteBuffer> generateBinaryVectors(int count) {
        Random ran = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        int byteCount = dimension / 8;
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = ByteBuffer.allocate(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
            }
            vectors.add(vector);
        }
        return vectors;

    }

    @Test
    void testFloatVectors() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "long_field";
        String field2Name = "vec_field";
        String field3Name = "bool_field";
        String field4Name = "double_field";
        String field5Name = "int_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Bool)
                .withName(field3Name)
                .withDescription("gender")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Double)
                .withName(field4Name)
                .withDescription("weight")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Int8)
                .withName(field5Name)
                .withDescription("age")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        R<ShowPartitionsResponse> spResp = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        System.out.println(spResp);

        ShowPartResponseWrapper wra = new ShowPartResponseWrapper(spResp.getData());
        List<ShowPartResponseWrapper.PartitionInfo> parts = wra.getPartitionsInfo();
        System.out.println("Partition num: "+parts.size());


        // insert data
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        List<Boolean> genders = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        List<Short> ages = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
            genders.add(i % 3 == 0 ? Boolean.TRUE : Boolean.FALSE);
            weights.add(((double) (i + 1) / 100));
            ages.add((short) ((i + 1) % 99));
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field5Name, ages));
        fieldsInsert.add(new InsertParam.Field(field4Name, weights));
        fieldsInsert.add(new InsertParam.Field(field3Name, genders));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
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
        System.out.println("Partition row count: " + statPart.getRowCount());

        // create index on scalar field
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field5Name)
                .withIndexType(IndexType.STL_SORT)
                .withSyncMode(Boolean.TRUE)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // create index on vector field
        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
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
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        DescIndexResponseWrapper indexDesc = new DescIndexResponseWrapper(descIndexR.getData());
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

        // query vectors to verify
        List<Long> queryIDs = new ArrayList<>();
        List<Double> compareWeights = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            queryIDs.add(ids.get(i));
            compareWeights.add(weights.get(i));
        }
        String expr = field1Name + " in " + queryIDs.toString();
        List<String> outputFields = Arrays.asList(field1Name, field2Name, field3Name, field4Name, field5Name);
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

            if (fieldName.compareTo(field1Name) == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
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
        if (outputFields.contains(field2Name)) {
            Assertions.assertTrue(queryResultsWrapper.getFieldWrapper(field2Name).isVectorField());
            List<?> out = queryResultsWrapper.getFieldWrapper(field2Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(field3Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field3Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(field4Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field4Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
            for (Object o : out) {
                double d = (Double) o;
                Assertions.assertTrue(compareWeights.contains(d));
            }
        }

        // query with offset and limit
        int queryLimit = 5;
        expr = field5Name + " > 1"; // "age > 1"
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
        List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
        Assertions.assertEquals(queryLimit, out.size());

        // pick some vectors to search
        List<Long> targetVectorIDs = new ArrayList<>();
        List<List<Float>> targetVectors = new ArrayList<>();
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add(ids.get(i));
            targetVectors.add(vectors.get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .withParams("{\"ef\":64}")
                .addOutField(field4Name)
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
            Assertions.assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        List<?> fieldData = results.getFieldData(field4Name, 0);
        Assertions.assertEquals(topK, fieldData.size());
        fieldData = results.getFieldData(field4Name, nq - 1);
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
    void testBinaryVectors() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "field1";
        String field2Name = "field2";
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("hello")
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withDataType(DataType.BinaryVector)
                .withName(field2Name)
                .withDescription("world")
                .withDimension(dimension)
                .build();

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .addFieldType(field1)
                .addFieldType(field2)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        int rowCount = 10000;
        List<ByteBuffer> vectors = generateBinaryVectors(rowCount);
        // insert data by columns
        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(field2Name, vectors.subList(0, 5000))); // no need to provide id here since this field is auto_id

        R<MutationResult> insertR1 = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR1.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR1.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids1 = insertResultWrapper.getLongIDs(); // get returned IDs(generated by server-side)

        // Insert entities by rows
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 5000; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.add(field2Name, GSON_INSTANCE.toJsonTree(vectors.get(i).array()));
            rows.add(row);
        }

        R<MutationResult> insertR2 = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR2.getStatus().intValue());

        insertResultWrapper = new MutationResultWrapper(insertR2.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids2 = insertResultWrapper.getLongIDs(); // get returned IDs(generated by server-side)
        List<Long> ids = new ArrayList<>();
        ids1.forEach((id) -> { ids.add(id);});
        ids2.forEach((id) -> { ids.add(id);});
        Assertions.assertEquals(rowCount, ids.size());

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());
        Assertions.assertEquals(rowCount, stat.getRowCount());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withExtraParam("{\"nlist\":64}")
                .withMetricType(MetricType.JACCARD)
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

        // search with BIN_FLAT index
        int searchTarget = 99;
        List<ByteBuffer> oneVector = new ArrayList<>();
        oneVector.add(vectors.get(searchTarget));

        SearchParam searchOneParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.JACCARD)
                .withTopK(5)
                .withVectors(oneVector)
                .withVectorFieldName(field2Name)
                .addOutField(field2Name)
                .build();

        R<SearchResults> searchOne = client.search(searchOneParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchOne.getStatus().intValue());

        SearchResultsWrapper oneResult = new SearchResultsWrapper(searchOne.getData().getResults());
        List<SearchResultsWrapper.IDScore> oneScores = oneResult.getIDScore(0);
        System.out.println("The search result of id " + ids.get(searchTarget) + " with SUPERSTRUCTURE metric:");
        System.out.println(oneScores);

        // verify the output vector, the top1 item is equal to the target vector
        List<?> items = oneResult.getFieldData(field2Name, 0);
        Assertions.assertEquals(items.size(), 5);
        ByteBuffer firstItem = (ByteBuffer) items.get(0);
        for (int i = 0; i < firstItem.limit(); ++i) {
            Assertions.assertEquals(firstItem.get(i), vectors.get(searchTarget).get(i));
        }

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
                .withFieldName(field2Name)
                .withIndexName("abv")
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withExtraParam("{\"nlist\":64}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

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
            targetVectorIDs.add(ids.get(i));
            targetVectors.add(vectors.get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.HAMMING)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .withParams("{\"nprobe\":8}")
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
            Assertions.assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
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
        String field1Name = "long_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert async
        List<ListenableFuture<R<MutationResult>>> futureResponses = new ArrayList<>();
        int rowCount = 1000;
        for (long i = 0L; i < 10; ++i) {
            List<List<Float>> vectors = normalizeFloatVectors(generateFloatVectors(rowCount));
            List<InsertParam.Field> fieldsInsert = new ArrayList<>();
            fieldsInsert.add(new InsertParam.Field(field2Name, vectors));

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
                .withFieldName(field2Name)
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
        List<List<Float>> targetVectors = normalizeFloatVectors(generateFloatVectors(2));
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .build();

        ListenableFuture<R<SearchResults>> searchFuture = client.searchAsync(searchParam);

        // query async
        String expr = field1Name + " in " + queryIDs.toString();
        List<String> outputFields = Arrays.asList(field1Name, field2Name);
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
        String field1Name = "long_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
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
        String field1Name = "str_id";
        String field2Name = "vec_field";
        String field3Name = "str_field";
        String field4Name = "int_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.VarChar)
                .withName(field1Name)
                .withMaxLength(32)
                .withDescription("string identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName(field3Name)
                .withMaxLength(32)
                .withDescription("comment")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Int64)
                .withName(field4Name)
                .withDescription("sequence")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
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
        List<String> ids = new ArrayList<>();
        List<String> comments = new ArrayList<>();
        List<Long> sequences = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(generator.generate(8));
            comments.add(generator.generate(8));
            sequences.add(i);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field3Name, comments));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));
        fieldsInsert.add(new InsertParam.Field(field4Name, sequences));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
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
                .withFieldName(field3Name)
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
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
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
            queryIds.add(ids.get(i));
            queryItems.add(sequences.get(i));
        }
        String expr = field4Name + " in " + queryItems.toString();
        List<String> outputFields = Arrays.asList(field1Name, field3Name);
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

            if (fieldName.compareTo(field1Name) == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
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
            targetVectors.add(vectors.get(seq.intValue()));
        }
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .addOutField(field4Name)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + queryIds.get(i) + "):");
            System.out.println(scores);
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
        String field1Name = "idg_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(dimension)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
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
                testIndex(randomCollectionName, field2Name, type, metric, indexTypes.get(type), Boolean.TRUE);
                testIndex(randomCollectionName, field2Name, type, metric, indexTypes.get(type), Boolean.FALSE);
            }
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testBinaryVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "id_field";
        String field2Name = "vector_field";
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withDataType(DataType.BinaryVector)
                .withName(field2Name)
                .withDimension(dimension)
                .build();

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .addFieldType(field1)
                .addFieldType(field2)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        List<MetricType> flatMetricTypes = new ArrayList<>();
        flatMetricTypes.add(MetricType.HAMMING);
        flatMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : flatMetricTypes) {
            testIndex(randomCollectionName, field2Name, IndexType.BIN_FLAT, metric, "{}", Boolean.TRUE);
            testIndex(randomCollectionName, field2Name, IndexType.BIN_FLAT, metric, "{}", Boolean.FALSE);
        }

        List<MetricType> ivfMetricTypes = new ArrayList<>();
        ivfMetricTypes.add(MetricType.HAMMING);
        ivfMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : ivfMetricTypes) {
            testIndex(randomCollectionName, field2Name, IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.TRUE);
            testIndex(randomCollectionName, field2Name, IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.FALSE);
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testDynamicField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "id_field";
        String field2Name = "vec_field";
        String field3Name = "json_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.JSON)
                .withName(field3Name)
                .withDescription("info")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .withEnableDynamicField(true)
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
                .withFieldName(field2Name)
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
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(field1Name, i);
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));

            // JSON field
            JsonObject info = new JsonObject();
            info.addProperty("row_based_info", i);
            row.add(field3Name, info);

            // extra meta is automatically stored in dynamic field
            row.addProperty("row_based_extra", i % 3 == 0);
            row.addProperty(generator.generate(5), 100);

            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // insert data by column-based
        List<Long> ids = new ArrayList<>();
        List<JsonObject> infos = new ArrayList<>();
        List<JsonObject> dynamics = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(rowCount + i);
            JsonObject obj = new JsonObject();
            obj.addProperty("column_based_info", i);
            obj.addProperty(generator.generate(5), i);
            infos.add(obj);

            JsonObject dynamic = new JsonObject();
            dynamic.addProperty(String.format("column_based_extra_%d", i), i);
            dynamics.add(dynamic);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));
        fieldsInsert.add(new InsertParam.Field(field3Name, infos));
        fieldsInsert.add(new InsertParam.Field(Constant.DYNAMIC_FIELD_NAME, dynamics));

        InsertParam insertColumnsParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
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
        String expr = "row_based_extra == true";
        List<String> outputFields = Arrays.asList(field3Name, "row_based_extra");
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
            Object extraMeta = record.get("row_based_extra");
            Assertions.assertInstanceOf(Boolean.class, extraMeta);
            System.out.println("'row_based_extra' is from dynamic field, value: " + extraMeta);
        }

        // search the No.11 and No.15
        List<List<Float>> targetVectors = new ArrayList<>();
        targetVectors.add(vectors.get(1));
        targetVectors.add(vectors.get(5));
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
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
            for (SearchResultsWrapper.IDScore score:scores) {
                System.out.println(score);
                Object extraMeta = score.get("row_based_extra");
                if (extraMeta != null) {
                    System.out.println("'row_based_extra' is from dynamic field, value: " + extraMeta);
                }
            }
        }
        Assertions.assertEquals(results.getIDScore(0).get(0).getLongID(), 11L);
        Assertions.assertEquals(results.getIDScore(1).get(0).getLongID(), 15L);

        // retrieve dynamic values inserted by column-based
        expr = "column_based_extra_1 == 1";
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("column_based_extra_1 == 1")
                .withOutFields(Collections.singletonList("*"))
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results with expr: " + expr);
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            long id = (long)record.get(field1Name);
            Assertions.assertEquals((long)rowCount+1L, id);
            Object vec = record.get(field2Name);
            Assertions.assertInstanceOf(List.class, vec);
            List<Float> vector = (List<Float>)vec;
            Assertions.assertEquals(dimension, vector.size());
            Object j = record.get(field3Name);
            Assertions.assertInstanceOf(JsonObject.class, j);
            JsonObject jon = (JsonObject)j;
            Assertions.assertTrue(jon.has("column_based_info"));
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
        String field1Name = "id_field";
        String field2Name = "vec_field";
        String field3Name = "str_array_field";
        String field4Name = "int_array_field";
        String field5Name = "float_array_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Array)
                .withElementType(DataType.VarChar)
                .withName(field3Name)
                .withMaxLength(256)
                .withMaxCapacity(300)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Array)
                .withElementType(DataType.Int32)
                .withName(field4Name)
                .withMaxCapacity(400)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Array)
                .withElementType(DataType.Float)
                .withName(field5Name)
                .withMaxCapacity(500)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
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
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));
        fieldsInsert.add(new InsertParam.Field(field3Name, strArrArray));
        fieldsInsert.add(new InsertParam.Field(field4Name, intArrArray));
        fieldsInsert.add(new InsertParam.Field(field5Name, floatArrArray));

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
            row.addProperty(field1Name, 10000L + (long)i);
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));

            List<String> strArray = new ArrayList<>();
            List<Integer> intArray = new ArrayList<>();
            List<Float> floatArray = new ArrayList<>();
            for (int k = 0; k < i; k++) {
                strArray.add(String.format("R_StringArray_%d_%d", i, k));
                intArray.add(i*10000 + k);
                floatArray.add((float)k/1000 + i);
            }
            row.add(field3Name, GSON_INSTANCE.toJsonTree(strArray));
            row.add(field4Name, GSON_INSTANCE.toJsonTree(intArray));
            row.add(field5Name, GSON_INSTANCE.toJsonTree(floatArray));

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
        List<List<Float>> searchVectors = generateFloatVectors(1);
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(5)
                .withVectors(searchVectors)
                .withVectorFieldName(field2Name)
                .addOutField(field3Name)
                .addOutField(field4Name)
                .addOutField(field5Name)
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
            List<?> strArray = (List<?>)score.get(field3Name);
            Assertions.assertEquals(id%10000, (long)strArray.size());
            List<?> intArray = (List<?>)score.get(field4Name);
            Assertions.assertEquals(id%10000, (long)intArray.size());
            List<?> floatArray = (List<?>)score.get(field5Name);
            Assertions.assertEquals(id%10000, (long)floatArray.size());
        }

        // search with array_contains
        searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withExpr(String.format("array_contains_any(%s, [450038, 680015])", field4Name))
                .withVectors(searchVectors)
                .withVectorFieldName(field2Name)
                .addOutField(field3Name)
                .addOutField(field4Name)
                .addOutField(field5Name)
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
        String field1Name = "id_field";
        String field2Name = "vec_field";
        String field3Name = "varchar_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName(field3Name)
                .withDescription("name")
                .withMaxLength(100)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .withEnableDynamicField(true)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data by row-based with id from 0 ~ 9
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(field1Name, i);
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));
            row.addProperty(field3Name, String.format("name_%d", i));
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
                .withFieldName(field2Name)
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
                .withExpr(String.format("%s == 5", field1Name))
                .addOutField(field3Name)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in sealed segment:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object name = record.get(field3Name);
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_5", name);
        }

        // insert 10 rows into growing segment with id from 10 ~ 19
        // since the ids are not exist, the upsert call is equal to an insert call
        rows.clear();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(field1Name, rowCount + i);
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));
            row.addProperty(field3Name, String.format("name_%d", rowCount + i));
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
                .withExpr(String.format("%s == 18", field1Name))
                .addOutField(field3Name)
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
            Object name = record.get(field3Name);
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_18", name);
            Assertions.assertThrows(ParamException.class, () -> record.get("dynamic_value")); // we didn't set dynamic_value for No.18 row
        }

        // upsert to change the no.5 and no.18 items
        rows.clear();
        JsonObject row = new JsonObject();
        row.addProperty(field1Name, 5L);
        List<Float> vector = generateFloatVectors(1).get(0);
        row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));
        row.addProperty(field3Name, "updated_5");
        row.addProperty("dynamic_value", String.format("dynamic_%d", 5));
        rows.add(row);
        row = new JsonObject();
        row.addProperty(field1Name, 18L);
        vector = generateFloatVectors(1).get(0);
        row.add(field2Name, GSON_INSTANCE.toJsonTree(vector));
        row.addProperty(field3Name, "updated_18");
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
                .withExpr(String.format("%s == 5 || %s == 18", field1Name, field1Name))
                .addOutField(field3Name)
                .addOutField("dynamic_value")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("updated_5", records.get(0).get(field3Name));
        Assertions.assertEquals("dynamic_5", records.get(0).get("dynamic_value"));
        Assertions.assertEquals("updated_18", records.get(1).get(field3Name));
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
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withDataType(DataType.Int64)
                .withName("id")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(dimension)
                .build());

        // create collection A
        R<RpcStatus> response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_A")
                .withFieldTypes(fieldsSchema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        // create collection B
        response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_B")
                .withFieldTypes(fieldsSchema)
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
                .withDimension(dimension)
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
//                .withDimension(dimension)
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
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), GSON_INSTANCE.toJsonTree(vector));
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
            List<Float> vector = generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), GSON_INSTANCE.toJsonTree(vector));
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
    public void testBulkWriter() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName("id")
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(dimension)
                .build();

        FieldType field3 = FieldType.newBuilder()
                .withDataType(DataType.Bool)
                .withName("bool")
                .build();

        FieldType field4 = FieldType.newBuilder()
                .withDataType(DataType.Int16)
                .withName("int16")
                .build();

        FieldType field5 = FieldType.newBuilder()
                .withDataType(DataType.Float)
                .withName("float")
                .build();

        FieldType field6 = FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName("varchar")
                .withMaxLength(100)
                .build();

        FieldType field7 = FieldType.newBuilder()
                .withDataType(DataType.JSON)
                .withName("json")
                .build();

        FieldType field8 = FieldType.newBuilder()
                .withDataType(DataType.Array)
                .withElementType(DataType.Int32)
                .withName("array")
                .withMaxCapacity(100)
                .build();

        CollectionSchemaParam schema = CollectionSchemaParam.newBuilder()
                .withFieldTypes(Lists.newArrayList(field1, field2, field3, field4, field5, field6, field7, field8))
                .withEnableDynamicField(false)
                .build();

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // local bulkwriter
        LocalBulkWriterParam localWriterParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schema)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(4 * 1024 * 1024)
                .build();

        int rowCount = 10000;
        List<List<String>> batchFiles = new ArrayList<>();
        try (LocalBulkWriter localBulkWriter = new LocalBulkWriter(localWriterParam)) {
            for (int i = 0; i < rowCount; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", i);
                row.add("vector", GSON_INSTANCE.toJsonTree(GeneratorUtils.genFloatVector(dimension)));
                row.addProperty("bool", i % 3 == 0);
                row.addProperty("int16", i%65535);
                row.addProperty("float", i/3);
                row.addProperty("varchar", String.format("varchar_%d", i));
                JsonObject obj = new JsonObject();
                obj.addProperty("dummy", i);
                row.add("json", obj);
                row.addProperty("dynamic_1", i);
                row.addProperty("dynamic_2", String.format("dynamic_%d", i));
                row.add("array", GSON_INSTANCE.toJsonTree(Lists.newArrayList(5, 6, 3, 2, 1)));

                localBulkWriter.appendRow(row);
            }

            localBulkWriter.commit(false);
            List<List<String>> files = localBulkWriter.getBatchFiles();
            System.out.printf("LocalBulkWriter done! output local files: %s%n", files);
            Assertions.assertEquals(files.size(), 2);
            Assertions.assertEquals(files.get(0).size(), 1);
            batchFiles.addAll(files);
        } catch (Exception e) {
            System.out.println("LocalBulkWriter catch exception: " + e);
            Assertions.fail();
        }

        try {
            final int[] counter = {0};
            for (List<String> files : batchFiles) {
                new ParquetReaderUtils() {
                    @Override
                    public void readRecord(GenericData.Record record) {
                        counter[0]++;
                    }
                }.readParquet(files.get(0));
            }
            Assertions.assertEquals(rowCount, counter[0]);
        } catch (Exception e) {
            System.out.println("Verify parquet file catch exception: " + e);
            Assertions.fail();
        }
    }

    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);

        String field1Name = "str_id";
        String field2Name = "vec_field";
        String field3Name = "json_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.VarChar)
                .withName(field1Name)
                .withMaxLength(32)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.JSON)
                .withName(field3Name)
                .build());


        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .withEnableDynamicField(true)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data
        int rowCount = 1000;
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(field1Name, Long.toString(i));
            row.add(field2Name, gson.toJsonTree(generateFloatVectors(1).get(0)));
            JsonObject json = new JsonObject();
            if (i%2 == 0) {
                json.addProperty("even", true);
            }
            row.add(field3Name, json);
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
                .withFieldName(field2Name)
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
                Assertions.assertInstanceOf(String.class, record.get(field1Name));
                Object vec = record.get(field2Name);
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>)vec;
                Assertions.assertEquals(dimension, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(field3Name));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(300, counter);

        // search iterator
        List<List<Float>> vectors = generateFloatVectors(1);
        SearchIteratorParam.Builder searchIteratorParamBuilder = SearchIteratorParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withOutFields(Lists.newArrayList("*"))
                .withBatchSize(10L)
                .withVectorFieldName(field2Name)
                .withVectors(vectors)
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
                Assertions.assertInstanceOf(String.class, record.get(field1Name));
                Object vec = record.get(field2Name);
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>)vec;
                Assertions.assertEquals(dimension, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(field3Name));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(50, counter);
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
                .withDimension(dimension)
                .build());

        // create collection
        R<RpcStatus> createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert
        JsonObject row = new JsonObject();
        row.add("vector", GSON_INSTANCE.toJsonTree(generateFloatVectors(1).get(0)));
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
}
