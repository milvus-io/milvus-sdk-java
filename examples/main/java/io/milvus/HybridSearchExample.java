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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.*;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.GetCollStatResponseWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HybridSearchExample {
    private static final String HOST = "localhost";
    private static final int HOST_PORT = 19530;
    private static final String COLLECTION_NAME = "java_sdk_example_hybrid_search";
    private static final String ID_FIELD = "ID";

    private static final String FLOAT_VECTOR_FIELD = "float_vector";
    private static final Integer FLOAT_VECTOR_DIM = 128;
    private static final MetricType FLOAT_VECTOR_METRIC = MetricType.COSINE;

    private static final String BINARY_VECTOR_FIELD = "binary_vector";
    private static final Integer BINARY_VECTOR_DIM = 256;
    private static final MetricType BINARY_VECTOR_METRIC = MetricType.JACCARD;

    private static final String FLOAT16_VECTOR_FIELD = "float16_vector";
    private static final Integer FLOAT16_VECTOR_DIM = 256;
    private static final MetricType FLOAT16_VECTOR_METRIC = MetricType.L2;

    private static final String SPARSE_VECTOR_FIELD = "sparse_vector";
    private static final MetricType SPARSE_VECTOR_METRIC = MetricType.IP;

    private static void createCollection() {
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(HOST)
                .withPort(HOST_PORT)
                .build());

        // Define fields
        // There is a configuration in milvus.yaml to define the max vector fields in a collection
        // proxy.maxVectorFieldNum: 4
        // By default, the max vector fields number is 4
        // In v2.4.0 there is a known bug that sparse vectors and float16 vectors in one collection
        // will crash milvus, so we comment out float16 vector field here.
        // https://github.com/milvus-io/milvus/issues/31988
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName(FLOAT_VECTOR_FIELD)
                        .withDataType(DataType.FloatVector)
                        .withDimension(FLOAT_VECTOR_DIM)
                        .build(),
                FieldType.newBuilder()
                        .withName(BINARY_VECTOR_FIELD)
                        .withDataType(DataType.BinaryVector)
                        .withDimension(BINARY_VECTOR_DIM)
                        .build(),
//                FieldType.newBuilder()
//                        .withName(FLOAT16_VECTOR_FIELD)
//                        .withDataType(DataType.Float16Vector)
//                        .withDimension(FLOAT16_VECTOR_DIM)
//                        .build(),
                FieldType.newBuilder()
                        .withName(SPARSE_VECTOR_FIELD)
                        .withDataType(DataType.SparseFloatVector)
                        .build()
        );

        // Create the collection with multi vector fields
        R<RpcStatus> resp = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withSchema(CollectionSchemaParam.newBuilder().withFieldTypes(fieldsSchema).build())
                .build());
        CommonUtils.handleResponseStatus(resp);

        // Specify an index types on the vector fields.
        resp = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(FLOAT_VECTOR_FIELD)
                .withIndexType(IndexType.IVF_PQ)
                .withExtraParam("{\"nlist\":128, \"m\":16, \"nbits\":8}")
                .withMetricType(FLOAT_VECTOR_METRIC)
                .build());
        CommonUtils.handleResponseStatus(resp);

        resp = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(BINARY_VECTOR_FIELD)
                .withIndexType(IndexType.BIN_FLAT)
                .withMetricType(BINARY_VECTOR_METRIC)
                .build());
        CommonUtils.handleResponseStatus(resp);

//        resp = milvusClient.createIndex(CreateIndexParam.newBuilder()
//                .withCollectionName(COLLECTION_NAME)
//                .withFieldName(FLOAT16_VECTOR_FIELD)
//                .withIndexType(IndexType.IVF_FLAT)
//                .withExtraParam("{\"nlist\":128}")
//                .withMetricType(FLOAT16_VECTOR_METRIC)
//                .build());
//        CommonUtils.handleResponseStatus(resp);

        resp = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(SPARSE_VECTOR_FIELD)
                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                .withExtraParam("{\"drop_ratio_build\":0.2}")
                .withMetricType(SPARSE_VECTOR_METRIC)
                .build());
        CommonUtils.handleResponseStatus(resp);

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println("Collection created");

        milvusClient.close();
    }

    private static void insertData() {
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(HOST)
                .withPort(HOST_PORT)
                .build());

        long idCount = 0;
        int rowCount = 10000;
        // Insert entities by rows
        List<JSONObject> rows = new ArrayList<>();
        for (long i = 1L; i <= rowCount; ++i) {
            JSONObject row = new JSONObject();
            row.put(ID_FIELD, idCount++);
            row.put(FLOAT_VECTOR_FIELD, CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM));
            row.put(BINARY_VECTOR_FIELD, CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM));
//            row.put(FLOAT16_VECTOR_FIELD, CommonUtils.generateFloat16Vector(FLOAT16_VECTOR_DIM, false));
            row.put(SPARSE_VECTOR_FIELD, CommonUtils.generateSparseVector());
            rows.add(row);
        }

        R<MutationResult> resp = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        CommonUtils.handleResponseStatus(resp);

        System.out.printf("%d entities inserted by rows", rowCount);

        // Insert entities by columns
        List<Long> ids = new ArrayList<>();
        for (long i = 1L; i <= 10000; ++i) {
            ids.add(idCount++);
        }

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(ID_FIELD, ids));
        fieldsInsert.add(new InsertParam.Field(FLOAT_VECTOR_FIELD,
                CommonUtils.generateFloatVectors(FLOAT_VECTOR_DIM, rowCount)));
        fieldsInsert.add(new InsertParam.Field(BINARY_VECTOR_FIELD,
                CommonUtils.generateBinaryVectors(BINARY_VECTOR_DIM, rowCount)));
//        fieldsInsert.add(new InsertParam.Field(FLOAT16_VECTOR_FIELD,
//                CommonUtils.generateFloat16Vectors(FLOAT16_VECTOR_DIM, rowCount, false)));
        fieldsInsert.add(new InsertParam.Field(SPARSE_VECTOR_FIELD,
                CommonUtils.generateSparseVectors(rowCount)));

        resp = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build());
        CommonUtils.handleResponseStatus(resp);

        System.out.printf("%d entities inserted by columns", rowCount);

        milvusClient.close();
    }

    private static void hybridSearch() {
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(HOST)
                .withPort(HOST_PORT)
                .build());

        // Get the row count
        R<GetCollectionStatisticsResponse> resp = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFlush(true)
                .build());
        CommonUtils.handleResponseStatus(resp);

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(resp.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // Search on multiple vector fields
        // Note that only allow one vector for each sub request
        AnnSearchParam req1 = AnnSearchParam.newBuilder()
                .withVectorFieldName(FLOAT_VECTOR_FIELD)
                .withVectors(CommonUtils.generateFloatVectors(FLOAT_VECTOR_DIM, 1))
                .withMetricType(FLOAT_VECTOR_METRIC)
                .withParams("{\"nprobe\": 32}")
                .withTopK(10)
                .build();

        AnnSearchParam req2 = AnnSearchParam.newBuilder()
                .withVectorFieldName(BINARY_VECTOR_FIELD)
                .withVectors(CommonUtils.generateBinaryVectors(BINARY_VECTOR_DIM, 1))
                .withMetricType(BINARY_VECTOR_METRIC)
                .withTopK(15)
                .build();

//        AnnSearchParam req3 = AnnSearchParam.newBuilder()
//                .withVectorFieldName(FLOAT16_VECTOR_FIELD)
//                .withVectors(CommonUtils.generateFloat16Vectors(FLOAT16_VECTOR_DIM, 1, false))
//                .withMetricType(FLOAT16_VECTOR_METRIC)
//                .withParams("{\"es\":200}")
//                .withTopK(20)
//                .build();

        AnnSearchParam req4 = AnnSearchParam.newBuilder()
                .withVectorFieldName(SPARSE_VECTOR_FIELD)
                .withVectors(CommonUtils.generateSparseVectors(1))
                .withMetricType(SPARSE_VECTOR_METRIC)
                .withParams("{\"drop_ratio_search\":0.2}")
                .withTopK(20)
                .build();

        HybridSearchParam searchParam = HybridSearchParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .addOutField(FLOAT_VECTOR_FIELD)
                .addOutField(BINARY_VECTOR_FIELD)
//                .addOutField(FLOAT16_VECTOR_FIELD)
                .addOutField(SPARSE_VECTOR_FIELD)
                .addSearchRequest(req1)
                .addSearchRequest(req2)
//                .addSearchRequest(req3)
                .addSearchRequest(req4)
                .withTopK(5)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withRanker(RRFRanker.newBuilder()
                        .withK(2)
                        .build())
                .build();

        R<SearchResults> searchR = milvusClient.hybridSearch(searchParam);
        CommonUtils.handleResponseStatus(searchR);

        // Print search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
        for (int i = 0; i < scores.size(); ++i) {
            System.out.println(scores.get(i));
        }


        milvusClient.close();
    }

    private static void dropCollection() {
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(HOST)
                .withPort(HOST_PORT)
                .build());

        R<RpcStatus> resp = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(resp);

        System.out.println("Collection dropped");

        milvusClient.close();
    }

    public static void main(String[] args) {
        createCollection();
        insertData();
        hybridSearch();
        dropCollection();
    }
}
