package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.collection.FlushParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.*;

public class BF16SearchIssue {
    private static int dim = 128;
    private static String uri="http://10.102.9.177:19530";
    private static String collectionName = "bf16Collection";
    private static String pkName = "fieldInt64";
    private static String vectorName = "fieldBFloat16Vector";

    public static void main(String[] args) {
        // connect
        MilvusClientV2 milvusClientV2 = new MilvusClientV2(ConnectConfig.builder()
                .uri(uri)
                .token("root:Milvus")
                .secure(false)
                .connectTimeoutMs(5000L)
                .build());
        // create collection
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(pkName)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .isPrimaryKey(false)
                .dimension(dim)
                .name(vectorName)
                .build();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(false)
                .description("collection desc")
                .numShards(1)
                .build();
        milvusClientV2.createCollection(createCollectionReq);
        // create index
        Map<String,Object> extraParamMap=new HashMap<String,Object>(){{
            put("M", 16);
            put("efConstruction", 64);
        }};
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParamMap)
                .build();

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        // load
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
        // insert
        List<JsonObject> jsonObjects = generateDefaultData(20000, dim);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(collectionName).data(jsonObjects).build());
        System.out.println(insert);
        //query
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .ids(Lists.newArrayList(1,10,99)).build());
        System.out.println("query:"+query.getQueryResults());
        // search
        List<BaseVector> data = providerBaseVector(1, dim);
        for (int i = 0; i < 10; i++) {
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .data(data)
                    .topK(10)
                    .build());
            System.out.println("first:"+search);
        }
        // use v1 flush
        MilvusServiceClient milvusClientV1 = new MilvusServiceClient(ConnectParam.newBuilder()
                .withUri(uri).build());
        milvusClientV1.flush(FlushParam.newBuilder().withCollectionNames(Lists.newArrayList(collectionName)).withSyncFlush(true).build());
        // search again
        for (int i = 0; i < 10; i++) {
            SearchResp search2 = milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .data(data)
                    .topK(10)
                    .build());
            System.out.println("second:" + search2);
        }
    }

    public static short floatToBF16(float value) {
        int bits = Float.floatToIntBits(value);
        int sign = bits >>> 31;
        int exp = (bits >>> 23) & 0xFF;
        int mantissa = bits & 0x7FFFFF;

        if (exp == 0xFF) {
            // 处理无穷大和 NaN
            if (mantissa == 0) {
                return (short) ((sign << 15) | 0x7C00);  // 正负无穷
            } else {
                return (short) ((sign << 15) | 0x7C00 | (mantissa >>> 13));  // NaN
            }
        }

        exp -= 127;
        exp += 16;
        if (exp >= 0x1F) {
            // 处理溢出情况
            return (short) ((sign << 15) | 0x7C00);
        } else if (exp <= 0) {
            // 处理下溢情况
            return 0;
        }

        return (short) ((sign << 15) | (exp << 10) | (mantissa >>> 13));
    }
    public static ByteBuffer generateBF16Vector(int dim) {
        Random ran = new Random();
        float randomFloat;
        int byteCount = dim * 2;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < dim; ++i) {
            do {
                randomFloat = ran.nextFloat();
            } while (Float.isInfinite(randomFloat));
            short halfFloatValue = MathUtil.floatToBF16(randomFloat);
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort(halfFloatValue);
            buffer.flip();
            vector.put(buffer.get(0));
            vector.put(buffer.get(1));
        }
        return vector;
    }

    public static List<JsonObject> generateDefaultData(long num, int dim) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0; i < num; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(pkName, i);
            row.add(vectorName, gson.toJsonTree(generateBF16Vector(dim).array()));
            jsonList.add(row);
        }
        return jsonList;
    }
    public static List<BaseVector> providerBaseVector(int nq, int dim) {
        List<BaseVector> data = new ArrayList<>();
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < nq; ++n) {
            vectors.add(generateBF16Vector(dim));
        }
        vectors.forEach(x -> {
            data.add(new BFloat16Vec(x));
        });

        return data;

    }
}
