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
package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.response.*;
import org.tensorflow.types.TBfloat16;
import org.tensorflow.types.TFloat16;

import java.nio.ByteBuffer;
import java.util.*;


public class Float16VectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_float16_vector_v1";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static final MilvusServiceClient milvusClient;
    static {
        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());
    }

    // For float16 values between 0.0~1.0, the precision can be controlled under 0.001f
    // For bfloat16 values between 0.0~1.0, the precision can be controlled under 0.01f
    private static boolean isFloat16Eauql(Float a, Float b, boolean bfloat16) {
        if (bfloat16) {
            return Math.abs(a - b) <= 0.01f;
        } else {
            return Math.abs(a - b) <= 0.001f;
        }
    }

    private static void createCollection(boolean bfloat16) {

        // drop the collection if you don't need the collection anymore
        R<Boolean> hasR = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(hasR);
        if (hasR.getData()) {
            dropCollection();
        }

        // Define fields
        DataType dataType = bfloat16 ? DataType.BFloat16Vector : DataType.Float16Vector;
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(dataType)
                        .withDimension(VECTOR_DIM)
                        .build()
        );

        // Create the collection
        // Note that we set default consistency level to "STRONG",
        // to ensure data is visible to search, for validation the result
        R<RpcStatus> ret = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withFieldTypes(fieldsSchema)
                .build());
        CommonUtils.handleResponseStatus(ret);
        System.out.println("Collection created");

        // Specify an index type on the vector field.
        ret = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":128}")
                .build());
        CommonUtils.handleResponseStatus(ret);
        System.out.println("Index created");

        // Call loadCollection() to enable automatically loading data into memory for searching
        ret = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(ret);
        System.out.println("Collection loaded");
    }

    private static void dropCollection() {
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");
    }

    private static void testFloat16(boolean bfloat16) {
        DataType dataType = bfloat16 ? DataType.BFloat16Vector : DataType.Float16Vector;
        System.out.printf("============ testFloat16 %s ===================\n", dataType.name());

        createCollection(bfloat16);

        // Insert 5000 entities by columns
        // Prepare original vectors, then encode into ByteBuffer
        int batchRowCount = 5000;
        List<List<Float>> originVectors = CommonUtils.generateFloatVectors(VECTOR_DIM, batchRowCount);
        List<ByteBuffer> encodedVectors = CommonUtils.encodeFloat16Vectors(originVectors, bfloat16);

        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < batchRowCount; ++i) {
            ids.add(i);
        }
        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(ID_FIELD, ids));
        fieldsInsert.add(new InsertParam.Field(VECTOR_FIELD, encodedVectors));

        R<MutationResult> insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build());
        CommonUtils.handleResponseStatus(insertR);
        System.out.println(ids.size() + " rows inserted");

        // Insert 5000 entities by rows
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < batchRowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, batchRowCount + i);

            List<Float> originVector = CommonUtils.generateFloatVector(VECTOR_DIM);
            originVectors.add(originVector);

            ByteBuffer buf = CommonUtils.encodeFloat16Vector(originVector, bfloat16);
            encodedVectors.add(buf);

            row.add(VECTOR_FIELD, gson.toJsonTree(buf.array()));
            rows.add(row);
        }

        insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        CommonUtils.handleResponseStatus(insertR);
        System.out.println(ids.size() + " rows inserted");

        // Pick some random vectors from the original vectors to search
        // Ensure the returned top1 item's ID should be equal to target vector's ID
        for (int i = 0; i < 10; i++) {
            Random ran = new Random();
            int k = ran.nextInt(batchRowCount*2);
            ByteBuffer targetVector = encodedVectors.get(k);
            SearchParam.Builder builder = SearchParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withMetricType(MetricType.L2)
                    .withLimit(3L)
                    .withVectorFieldName(VECTOR_FIELD)
                    .addOutField(VECTOR_FIELD)
                    .withParams("{\"nprobe\":32}");
            if (bfloat16) {
                builder.withBFloat16Vectors(Collections.singletonList(targetVector));
            } else {
                builder.withFloat16Vectors(Collections.singletonList(targetVector));
            }
            R<SearchResults> searchRet = milvusClient.search(builder.build());
            CommonUtils.handleResponseStatus(searchRet);

            // The search() allows multiple target vectors to search in a batch.
            // Here we only input one vector to search, get the result of No.0 vector to check
            SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchRet.getData().getResults());
            List<SearchResultsWrapper.IDScore> scores = resultsWrapper.getIDScore(0);
            System.out.printf("The result of No.%d target vector:\n", i);

            SearchResultsWrapper.IDScore firstScore = scores.get(0);
            if (firstScore.getLongID() != k) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d",
                        firstScore.getLongID(), k));
            }

            ByteBuffer outputBuf = (ByteBuffer)firstScore.get(VECTOR_FIELD);
            if (!outputBuf.equals(targetVector)) {
                throw new RuntimeException(String.format("The output vector is not equal to target vector: ID %d", k));
            }

            List<Float> outputVector = CommonUtils.decodeFloat16Vector(outputBuf, bfloat16);
            List<Float> originVector = originVectors.get(k);
            for (int j = 0; j < outputVector.size(); j++) {
                if (!isFloat16Eauql(outputVector.get(j), originVector.get(j), bfloat16)) {
                    throw new RuntimeException(String.format("The output vector is not equal to original vector: ID %d", k));
                }
            }
            System.out.println("\nTarget vector: " + originVector);
            System.out.println("Top0 result: " + firstScore);
            System.out.println("Top0 result vector: " + outputVector);
        }
        System.out.println("Search result is correct");

        // Retrieve some data and verify the output
        for (int i = 0; i < 10; i++) {
            Random ran = new Random();
            int k = ran.nextInt(batchRowCount*2);
            R<QueryResults> queryR = milvusClient.query(QueryParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withExpr(String.format("id == %d", k))
                    .addOutField(VECTOR_FIELD)
                    .build());
            CommonUtils.handleResponseStatus(queryR);
            QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryR.getData());
            FieldDataWrapper field = queryWrapper.getFieldWrapper(VECTOR_FIELD);
            List<?> r = field.getFieldData();
            if (r.isEmpty()) {
                throw new RuntimeException("The query result is empty");
            } else {
                ByteBuffer outputBuf = (ByteBuffer) r.get(0);
                ByteBuffer targetVector = encodedVectors.get(k);
                if (!outputBuf.equals(targetVector)) {
                    throw new RuntimeException("The query result is incorrect");
                }

                List<Float> outputVector = CommonUtils.decodeFloat16Vector(outputBuf, bfloat16);
                List<Float> originVector = originVectors.get(k);
                for (int j = 0; j < outputVector.size(); j++) {
                    if (!isFloat16Eauql(outputVector.get(j), originVector.get(j), bfloat16)) {
                        throw new RuntimeException(String.format("The output vector is not equal to original vector: ID %d", k));
                    }
                }
            }
        }
        System.out.println("Query result is correct");

        // drop the collection if you don't need the collection anymore
        dropCollection();
    }

    private static void testTensorflowFloat16(boolean bfloat16) {
        DataType dataType = bfloat16 ? DataType.BFloat16Vector : DataType.Float16Vector;
        System.out.printf("============ testTensorflowFloat16 %s ===================\n", dataType.name());
        createCollection(bfloat16);

        // Prepare tensorflow vectors, convert to ByteBuffer and insert
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
        }
        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(ID_FIELD, ids));

        List<ByteBuffer> encodedVectors;
        if (bfloat16) {
            List<TBfloat16> tfVectors = CommonUtils.genTensorflowBF16Vectors(VECTOR_DIM, rowCount);
            encodedVectors = CommonUtils.encodeTensorBF16Vectors(tfVectors);
        } else {
            List<TFloat16> tfVectors = CommonUtils.genTensorflowFP16Vectors(VECTOR_DIM, rowCount);
            encodedVectors = CommonUtils.encodeTensorFP16Vectors(tfVectors);
        }
        fieldsInsert.add(new InsertParam.Field(VECTOR_FIELD, encodedVectors));

        R<MutationResult> insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build());
        CommonUtils.handleResponseStatus(insertR);
        System.out.println(ids.size() + " rows inserted");

        // Retrieve some data and verify the output
        Random ran = new Random();
        int k = ran.nextInt(rowCount);
        R<QueryResults> queryR = milvusClient.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(String.format("id == %d", k))
                .addOutField(VECTOR_FIELD)
                .build());
        CommonUtils.handleResponseStatus(queryR);
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryR.getData());
        FieldDataWrapper field = queryWrapper.getFieldWrapper(VECTOR_FIELD);
        List<?> r = field.getFieldData();
        if (r.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        }

        ByteBuffer outputBuf = (ByteBuffer) r.get(0);
        ByteBuffer originVector = encodedVectors.get(k);
        if (!outputBuf.equals(originVector)) {
            throw new RuntimeException("The query result is incorrect");
        }

        List<Float> outVector;
        if (bfloat16) {
            outVector = CommonUtils.decodeBF16VectorToFloat(outputBuf);
        } else {
            outVector = CommonUtils.decodeFP16VectorToFloat(outputBuf);
        }
        System.out.println("Output vector: " + outVector);
        System.out.println("Query result is correct");

        // drop the collection if you don't need the collection anymore
        dropCollection();
    }

    public static void main(String[] args) {
        testFloat16(true);
        testFloat16(false);

        testTensorflowFloat16(true);
        testTensorflowFloat16(false);
    }
}
