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

package io.milvus.system.v1.client;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.WeightedRanker;
import io.milvus.param.index.*;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Tag("system")
class VectorDockerTest extends MilvusV1DockerTestBase {

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
        System.out.println("Partition num: " + parts.size());

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
        String params = "{\"efConstruction\":64,\"M\":16,\"mmap.enabled\":true}";
        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexName("abv")
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.L2)
                .withExtraParam(params)
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
        String extraParams = indexDesc.getExtraParam();
        Assertions.assertEquals(params.replace("\"", ""), extraParams.replace("\"", ""));
        System.out.println("Index description: " + indexDesc);

        R<RpcStatus> alterR = client.alterIndex(AlterIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName("abv")
                .withMMapEnabled(false)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), alterR.getStatus().intValue());

        descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());
        indexDescWrapper = new DescIndexResponseWrapper(descIndexR.getData());
        indexDesc = indexDescWrapper.getIndexDescByFieldName(DataType.FloatVector.name());
        extraParams = indexDesc.getExtraParam();
        Assertions.assertEquals("{efConstruction:64,M:16,mmap.enabled:false}", extraParams.replace("\"", ""));

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
        System.out.println("Collection info: " + info);

        // show partitions
        R<ShowPartitionsResponse> showPartR = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addPartitionName("_default") // each collection has a '_default' partition
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), showPartR.getStatus().intValue());
        ShowPartResponseWrapper infoPart = new ShowPartResponseWrapper(showPartR.getData());
        System.out.println("Partition info: " + infoPart);

        // query
        Long fetchID = 100L;
        List<Float> fetchVector = (List<Float>) columnsData.get(1).getValues().get(fetchID.intValue());
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
            queryIDs.add((Long) columnsData.get(0).getValues().get(i));
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
//            System.out.println(wrapper.getFieldData());
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
            targetVectorIDs.add((Long) columnsData.get(0).getValues().get(i));
            targetVectors.add((List<Float>) columnsData.get(1).getValues().get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withLimit((long) topK)
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
//            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
//            System.out.println(scores);
            Assertions.assertEquals(targetVectorIDs.get(i), scores.get(0).getLongID());

            Object obj = scores.get(0).get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, obj);
            List<Float> outputVec = (List<Float>) obj;
            Assertions.assertEquals(targetVectors.get(i).size(), outputVec.size());
            for (int k = 0; k < outputVec.size(); k++) {
                Assertions.assertEquals(targetVectors.get(i).get(k), outputVec.get(k));
            }

            // verify the old way
            List<QueryResultsWrapper.RowRecord> records = results.getRowRecords(i);
            obj = records.get(0).get(DataType.FloatVector.name());
            outputVec = (List<Float>) obj;
            Assertions.assertEquals(targetVectors.get(i).size(), outputVec.size());
            for (int k = 0; k < outputVec.size(); k++) {
                Assertions.assertEquals(targetVectors.get(i).get(k), outputVec.get(k));
            }
            double d = (double) records.get(0).get(DataType.Double.name());
            Assertions.assertEquals(d, compareWeights.get(i));
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
        byte[] byteArray = new byte[DIMENSION / 8];
        for (int i = 0; i < byteArray.length; i++) {
            byteArray[i] = (byte) ((i % 3 == 0) ? 255 : 0);
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
        Assertions.assertEquals(2 * rowCount + 1, stat.getRowCount());

        // check index
        while (true) {
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
            Assertions.assertEquals(2 * rowCount + 1, indexDesc.getTotalRows());
            Assertions.assertEquals(2 * rowCount + 1, indexDesc.getIndexedRows());
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
        ByteBuffer fetchVector = (ByteBuffer) columnsData.get(0).getValues().get(0);
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
        ByteBuffer targetVector = (ByteBuffer) columnsData.get(0).getValues().get(searchTarget);

        SearchParam searchOneParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.JACCARD)
                .withLimit(5L)
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
                .withLimit((long) topK)
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
//            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
//            System.out.println(scores);
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
        Long fetchID = (Long) columnsData.get(0).getValues().get(0);
        SortedMap<Long, Float> fetchVector = (SortedMap<Long, Float>) columnsData.get(1).getValues().get(0);
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
            targetVectorIDs.add((Long) columnsData.get(0).getValues().get(i));
            targetVectors.add((SortedMap<Long, Float>) columnsData.get(1).getValues().get(i));
        }

        System.out.println("Search target IDs:" + targetVectorIDs);
        System.out.println("Search target vectors:" + targetVectors);

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withLimit((long) topK)
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
//            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
//            System.out.println(scores);
//            if (targetVectorIDs.get(i) != scores.get(0).getLongID()) {
//                System.out.println(targetVectors.get(i));
//            }
            Assertions.assertEquals(targetVectorIDs.get(i), scores.get(0).getLongID());

            Object v = scores.get(0).get(DataType.SparseFloatVector.name());
            SortedMap<Long, Float> sparse = (SortedMap<Long, Float>) v;
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
            ids.add((long) i);
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
                .withLimit((long) topK)
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
        List<Float> fp16Vec = Float16Utils.fp16BufferToVector((ByteBuffer) v);
        Assertions.assertEquals(fp16Vec.size(), originVector.size());
        for (int k = 0; k < fp16Vec.size(); k++) {
            Assertions.assertTrue(Math.abs(fp16Vec.get(k) - originVector.get(k)) <= FLOAT16_PRECISION);
        }

        // search bfloat16 vector
        ByteBuffer bf16Vector = Float16Utils.f32VectorToBf16Buffer(vectors.get((int) targetID));
        searchR = client.search(SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withLimit((long) topK)
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
        List<Float> bf16Vec = Float16Utils.bf16BufferToVector((ByteBuffer) v);
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

        // prepare sub requests
        int nq = 5;
        long topk = 10L;
        Function<Integer, HybridSearchParam> genRequestFunc =
                sparseCount -> {
                    AnnSearchParam param1 = AnnSearchParam.newBuilder()
                            .withVectorFieldName(DataType.FloatVector.name())
                            .withFloatVectors(utils.generateFloatVectors(nq))
                            .withMetricType(MetricType.COSINE)
                            .withParams("{\"nprobe\": 32}")
                            .withLimit(15L)
                            .build();

                    AnnSearchParam param2 = AnnSearchParam.newBuilder()
                            .withVectorFieldName(DataType.BinaryVector.name())
                            .withBinaryVectors(utils.generateBinaryVectors(nq))
                            .withMetricType(MetricType.HAMMING)
                            .withParams("{}")
                            .withLimit(5L)
                            .build();

                    List<SortedMap<Long, Float>> sparseVEctors = sparseCount > 0 ?
                            utils.generateSparseVectors(sparseCount) : new ArrayList<>();
                    AnnSearchParam param3 = AnnSearchParam.newBuilder()
                            .withVectorFieldName(DataType.SparseFloatVector.name())
                            .withSparseFloatVectors(sparseVEctors)
                            .withMetricType(MetricType.IP)
                            .withParams("{\"drop_ratio_search\":0.2}")
                            .withLimit(7L)
                            .build();

                    // search with an empty nq, return error
                    return HybridSearchParam.newBuilder()
                            .withCollectionName(randomCollectionName)
                            .addOutField(DataType.SparseFloatVector.name())
                            .addSearchRequest(param1)
                            .addSearchRequest(param2)
                            .addSearchRequest(param3)
                            .withLimit(topk)
                            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                            .withRanker(WeightedRanker.newBuilder()
                                    .withWeights(Lists.newArrayList(0.5f, 0.5f, 1.0f))
                                    .build())
                            .withOutFields(Collections.singletonList("*"))
                            .build();
                };

        // search with an empty nq, return error
        Assertions.assertThrows(ParamException.class, () -> genRequestFunc.apply(0));

        // unequal nq, return error
        Assertions.assertThrows(ParamException.class, () -> genRequestFunc.apply(1));

        // TODO: comment out these lines because current milvus master has bug in hybrid-search empty collection
//        // search on empty collection, no result returned
//        R<SearchResults> searchR = client.hybridSearch(genRequestFunc.apply(nq));
//        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());
//        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
//        for (int i = 0; i < results.getNumQueries(); ++i) {
//            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
//            Assertions.assertTrue(scores.isEmpty());
//        }

        // insert data to multiple vector fields
        int rowCount = 10000;
        List<InsertParam.Field> fields = generateColumnsData(schema, rowCount, 0);
        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();
        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // search on multiple vector fields
        R<SearchResults> searchR = client.hybridSearch(genRequestFunc.apply(nq));
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // check search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        for (SearchResultsWrapper.IDScore score : scores) {
            Object id = score.get("id");
            Assertions.assertInstanceOf(Long.class, id);
            Object fv = score.get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, fv);
            List<Float> fvec = (List<Float>) fv;
            Assertions.assertEquals(DIMENSION, fvec.size());
            Object bv = score.get(DataType.BinaryVector.name());
            Assertions.assertInstanceOf(ByteBuffer.class, bv);
            ByteBuffer bvec = (ByteBuffer) bv;
            Assertions.assertEquals(DIMENSION, bvec.limit() * 8);
            Object sv = score.get(DataType.SparseFloatVector.name());
            Assertions.assertInstanceOf(SortedMap.class, sv);
        }
        for (int i = 0; i < results.getNumQueries(); ++i) {
            scores = results.getIDScore(i);
            Assertions.assertEquals(topk, scores.size());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
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
        System.out.println(desc);

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
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
            Object extraMeta = record.get("dynamic");
            Assertions.assertInstanceOf(Long.class, extraMeta);
            Assertions.assertTrue(target.contains(extraMeta));
            System.out.println("'dynamic' is from dynamic field, value: " + extraMeta);
        }

        // search the No.11 and No.15
        target = Arrays.asList(1L, 5L);
        List<List<Float>> targetVectors = new ArrayList<>();
        targetVectors.add((List<Float>) columnsData.get(1).getValues().get(target.get(0).intValue()));
        targetVectors.add((List<Float>) columnsData.get(1).getValues().get(target.get(1).intValue()));
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.COSINE)
                .withLimit((long) topK)
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
//            System.out.println("The result of No." + i + " target vector:");
//            Assertions.assertFalse(scores.isEmpty());
            SearchResultsWrapper.IDScore score = scores.get(0);
            System.out.println(score);
            Object extraMeta = score.get("dynamic");
            Assertions.assertInstanceOf(Long.class, extraMeta);
            Long k = (Long) extraMeta - rowCount;
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
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
            long id = (long) record.get("id");
            Assertions.assertEquals(18L, id);
            Object vec = record.get(DataType.FloatVector.name());
            Assertions.assertInstanceOf(List.class, vec);
            List<Float> vector = (List<Float>) vec;
            Assertions.assertEquals(DIMENSION, vector.size());
            Object j = record.get(DataType.JSON.name());
            Assertions.assertInstanceOf(JsonObject.class, j);
            JsonObject jon = (JsonObject) j;
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
            ids.add((long) i);
            List<String> strArray = new ArrayList<>();
            List<Integer> intArray = new ArrayList<>();
            List<Float> floatArray = new ArrayList<>();
            for (int k = 0; k < i; k++) {
                strArray.add(String.format("C_StringArray_%d_%d", i, k));
                intArray.add(i * 10000 + k);
                floatArray.add((float) k / 1000 + i);
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
            row.addProperty("id", 10000L + (long) i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));

            List<String> strArray = new ArrayList<>();
            List<Integer> intArray = new ArrayList<>();
            List<Float> floatArray = new ArrayList<>();
            for (int k = 0; k < i; k++) {
                strArray.add(String.format("R_StringArray_%d_%d", i, k));
                intArray.add(i * 10000 + k);
                floatArray.add((float) k / 1000 + i);
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
                .withLimit(5L)
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
//        System.out.println("Search results:");
        for (SearchResultsWrapper.IDScore score : scores) {
//            System.out.println(score);
            long id = score.getLongID();
            List<?> strArray = (List<?>) score.get(varcharArrayName);
            Assertions.assertEquals(id % 10000, strArray.size());
            List<?> intArray = (List<?>) score.get(intArrayName);
            Assertions.assertEquals(id % 10000, intArray.size());
            List<?> floatArray = (List<?>) score.get(floatArrayName);
            Assertions.assertEquals(id % 10000, floatArray.size());
        }

        // search with array_contains
        searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withLimit(10L)
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
//        System.out.println("Search results:");
        for (SearchResultsWrapper.IDScore score : scores) {
//            System.out.println(score);
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
        System.out.println(desc);

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
            queryIds.add((String) columnsData.get(0).getValues().get(i));
            queryItems.add((Long) columnsData.get(3).getValues().get(i));
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
//            System.out.println(wrapper.getFieldData());
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
            targetVectors.add((List<Float>) columnsData.get(1).getValues().get(seq.intValue()));
        }
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withLimit((long) topK)
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
//            System.out.println("The result of No." + i + " target vector(ID = " + queryIds.get(i) + "):");
//            System.out.println(scores);
            Assertions.assertEquals(scores.get(0).getStrID(), queryIds.get(i));
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }
}
