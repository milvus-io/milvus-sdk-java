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

import java.nio.ByteBuffer;
import java.util.*;

import org.tensorflow.Tensor;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.types.TBfloat16;
import org.tensorflow.types.TFloat16;


public class Float16VectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_float16";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;
    

    private static void testFloat16(boolean bfloat16) {
        DataType dataType = bfloat16 ? DataType.BFloat16Vector : DataType.Float16Vector;
        System.out.printf("=================== %s ===================\n", dataType.name());

        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());

        // drop the collection if you don't need the collection anymore
        R<Boolean> hasR = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                                    .withCollectionName(COLLECTION_NAME)
                                    .build());
        CommonUtils.handleResponseStatus(hasR);
        if (hasR.getData()) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .build());
        }

        // Define fields
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
        R<RpcStatus> ret = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withFieldTypes(fieldsSchema)
                .build());
        CommonUtils.handleResponseStatus(ret);
        System.out.println("Collection created");

        // Insert entities by columns
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
        }
        List<ByteBuffer> vectors = CommonUtils.generateFloat16Vectors(VECTOR_DIM, rowCount, bfloat16);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(ID_FIELD, ids));
        fieldsInsert.add(new InsertParam.Field(VECTOR_FIELD, vectors));

        R<MutationResult> insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build());
        CommonUtils.handleResponseStatus(insertR);

        // Insert entities by rows
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 1L; i <= rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, rowCount + i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloat16Vector(VECTOR_DIM, bfloat16).array()));
            rows.add(row);
        }

        insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        CommonUtils.handleResponseStatus(insertR);

        // Flush the data to storage for testing purpose
        // Note that no need to manually call flush interface in practice
        R<FlushResponse> flushR = milvusClient.flush(FlushParam.newBuilder().
                addCollectionName(COLLECTION_NAME).
                build());
        CommonUtils.handleResponseStatus(flushR);
        System.out.println("Entities inserted");

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

        // Pick some vectors from the inserted vectors to search
        // Ensure the returned top1 item's ID should be equal to target vector's ID
        for (int i = 0; i < 10; i++) {
            Random ran = new Random();
            int k = ran.nextInt(rowCount);
            ByteBuffer targetVector = vectors.get(k);
            SearchParam.Builder builder = SearchParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withMetricType(MetricType.L2)
                    .withTopK(3)
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
            for (SearchResultsWrapper.IDScore score : scores) {
                System.out.println(score);
            }
            if (scores.get(0).getLongID() != k) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d",
                        scores.get(0).getLongID(), k));
            }
        }
        System.out.println("Search result is correct");

        // Retrieve some data
        int n = 99;
        R<QueryResults> queryR = milvusClient.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(String.format("id == %d", n))
                .addOutField(VECTOR_FIELD)
                .build());
        CommonUtils.handleResponseStatus(queryR);
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryR.getData());
        FieldDataWrapper field = queryWrapper.getFieldWrapper(VECTOR_FIELD);
        List<?> r = field.getFieldData();
        if (r.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        } else {
            ByteBuffer bf = (ByteBuffer) r.get(0);
            if (!bf.equals(vectors.get(n))) {
                throw new RuntimeException("The query result is incorrect");
            }
        }
        System.out.println("Query result is correct");

        // insert a single row
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, 9999999);
        List<Float> newVector = CommonUtils.generateFloatVector(VECTOR_DIM);
        ByteBuffer vector16Buf = encodeTF(newVector, bfloat16);
        row.add(VECTOR_FIELD, gson.toJsonTree(vector16Buf.array()));
        insertR = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(Collections.singletonList(row))
                .build());
        CommonUtils.handleResponseStatus(insertR);

        // retrieve the single row
        queryR = milvusClient.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("id == 9999999")
                .addOutField(VECTOR_FIELD)
                .build());
        CommonUtils.handleResponseStatus(queryR);
        queryWrapper = new QueryResultsWrapper(queryR.getData());
        field = queryWrapper.getFieldWrapper(VECTOR_FIELD);
        r = field.getFieldData();
        if (r.isEmpty()) {
            throw new RuntimeException("The retrieve result is empty");
        } else {
            ByteBuffer outBuf = (ByteBuffer) r.get(0);
            List<Float> outVector = decodeTF(outBuf, bfloat16);
            if (outVector.size() != newVector.size()) {
                throw new RuntimeException("The retrieve result is incorrect");
            }
            for (int i = 0; i < outVector.size(); i++) {
                if (!isFloat16Eauql(outVector.get(i), newVector.get(i), bfloat16)) {
                    throw new RuntimeException("The retrieve result is incorrect");
                }
            }
        }
        System.out.println("Retrieve result is correct");

        // drop the collection if you don't need the collection anymore
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");

        milvusClient.close();
    }

    private static ByteBuffer encodeTF(List<Float> vector, boolean bfloat16) {
        ByteBuffer buf = ByteBuffer.allocate(vector.size() * 2);
        for (Float value : vector) {
            ByteDataBuffer bf;
            if (bfloat16) {
                TBfloat16 tt = TBfloat16.scalarOf(value);
                bf = tt.asRawTensor().data();
            } else {
                TFloat16 tt = TFloat16.scalarOf(value);
                bf = tt.asRawTensor().data();
            }
            buf.put(bf.getByte(0));
            buf.put(bf.getByte(1));
        }
        return buf;
    }

    private static List<Float> decodeTF(ByteBuffer buf, boolean bfloat16) {
        int dim = buf.limit()/2;
        ByteDataBuffer bf = DataBuffers.of(buf.array());
        List<Float> vec = new ArrayList<>();
        if (bfloat16) {
            TBfloat16 tf = Tensor.of(TBfloat16.class, Shape.of(dim), bf);
            for (long k = 0; k < tf.size(); k++) {
                vec.add(tf.getFloat(k));
            }
        } else {
            TFloat16 tf = Tensor.of(TFloat16.class, Shape.of(dim), bf);
            for (long k = 0; k < tf.size(); k++) {
                vec.add(tf.getFloat(k));
            }
        }

        return vec;
    }

    private static boolean isFloat16Eauql(Float a, Float b, boolean bfloat16) {
        if (bfloat16) {
            return Math.abs(a - b) <= 0.01f;
        } else {
            return Math.abs(a - b) <= 0.001f;
        }
    }


    public static void main(String[] args) {
        testFloat16(true);
        testFloat16(false);
    }
}
