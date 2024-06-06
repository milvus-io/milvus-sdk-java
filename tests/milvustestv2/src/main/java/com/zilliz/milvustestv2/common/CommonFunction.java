package com.zilliz.milvustestv2.common;

import com.google.gson.*;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.utils.GenerateUtil;


import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2024/2/1 15:55
 */
@Slf4j
public class CommonFunction {

    /**
     * 创建float类型向量的collection
     * @param dim 维度
     * @param collectionName collection name
     * @return collection name
     */
    public static String createNewCollection(int dim, String collectionName) {
        if(collectionName==null){
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64=CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList=new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt16);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldArray);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldFloatVector);
        CreateCollectionReq.CollectionSchema collectionSchema= CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(false)
                .description("collection desc")
                .numShards(1)
                .build();
        BaseTest.milvusClientV2.createCollection(createCollectionReq);
        log.info("create collection:" + collectionName);
        return collectionName;
    }

    /**
     * 为float类型向量的collection提供导入的数据，目前只支持行式插入
     * @param num 数据量
     * @param dim 维度
     * @return List<JsonObject>
     */
    public static List<JsonObject> generateDefaultData(long num,int dim){
        List<JsonObject> jsonList=new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0; i < num; i++) {
            JsonObject row=new JsonObject();
            row.addProperty(CommonData.fieldInt64,i);
            row.addProperty(CommonData.fieldInt32,(int)i%32767);
            row.addProperty(CommonData.fieldInt16,(int)i%32767);
            row.addProperty(CommonData.fieldInt8,(short)i%127);
            row.addProperty(CommonData.fieldDouble,(double)i);
            row.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i,i+1,i+2)));
            row.addProperty(CommonData.fieldBool, i % 2 == 0);
            row.addProperty(CommonData.fieldVarchar,"Str"+i);
            row.addProperty(CommonData.fieldFloat,(float)i);
            List<Float> vector=new ArrayList<>();
            for (int k = 0; k < dim; ++k) {
                vector.add(ran.nextFloat());
            }
            row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            JsonObject json = new JsonObject();
            json.addProperty(CommonData.fieldInt64,(int)i%32767);
            json.addProperty(CommonData.fieldInt32,(int)i%32767);
            json.addProperty(CommonData.fieldDouble,(double)i);
            json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i,i+1,i+2)));
            json.addProperty(CommonData.fieldBool, i % 2 == 0);
            json.addProperty(CommonData.fieldVarchar,"Str"+i);
            json.addProperty(CommonData.fieldFloat,(float)i);
            row.add(CommonData.fieldJson,json);
            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * 为快速生成的collection提供导入数据
     * @param num 数据量
     * @param dim 维度
     * @return List<JsonObject>
     */
    public static List<JsonObject> generateSimpleData(long num,int dim){
        List<JsonObject> jsonList=new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0; i < num; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.simplePk, i);
            List<Float> vector=new ArrayList<>();
            for (int k = 0; k < dim; ++k) {
                vector.add(ran.nextFloat());
            }
            row.add(CommonData.simpleVector, gson.toJsonTree(vector));
            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * 快速创建一个collection，只有主键和向量字段
     * @param dim 维度
     * @param collectionName collection name
     * @return collectionName
     */
    public static String createSimpleCollection(int dim,String collectionName){
        if(collectionName==null){
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        BaseTest.milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .autoID(false)
                .dimension(dim)
                .enableDynamicField(false)
                .build());
        return collectionName;
    }

    /**
     * 创建索引时，提供额外的参数
     * @param indexType 索引类型
     * @return Map类型参数
     */
    public static Map<String,Object> provideExtraParam(IndexParam.IndexType indexType){
        Map<String,Object> map=new HashMap<>();
        switch (indexType){
            case FLAT:
            case AUTOINDEX:
                break;
            case HNSW:
                map.put("M",16);
                map.put("efConstruction",64);
                break;
            default:
                map.put("nlist",128);
                break;
        }
        return map;
    }

    /**
     * 创建向量索引
     * @param collectionName collectionName
     * @param vectorName 向量名称
     * @param indexType indexType
     * @param metricType metricType
     */
    public static void createVectorIndex(String collectionName,String vectorName, IndexParam.IndexType indexType, IndexParam.MetricType metricType){
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorName)
                .indexType(indexType)
                .extraParams(provideExtraParam(indexType))
                .metricType(metricType)
                .build();
        BaseTest.milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    /**
     * 创建标量索引
     * @param collectionName collectionName
     * @param scalarName 多个标量名称的集合
     */
    public static void createScalarIndex(String collectionName,List<String> scalarName){
        List<IndexParam> indexParams = new ArrayList<>();
        scalarName.forEach(x->{
            IndexParam indexParam=IndexParam.builder().indexType(IndexParam.IndexType.TRIE).fieldName(x).build();
            indexParams.add(indexParam);
        });
        BaseTest.milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(indexParams)
                .build());
    }


    public static void createPartition(String collectionName,String partitionName){
        BaseTest.milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(collectionName)
                .partitionName(partitionName)
                .build());
    }

    public static SearchResp defaultSearch(String collectionName){
        List<List<Float>> vectors = GenerateUtil.generateFloatVector(10, 3, CommonData.dim);
        List<BaseVector> data = new ArrayList<>();
        vectors.forEach((v)->{data.add(new FloatVec(v));});
        return BaseTest.milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(CommonData.topK)
                .build());
    }

}

