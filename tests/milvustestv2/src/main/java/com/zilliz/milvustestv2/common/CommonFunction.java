package com.zilliz.milvustestv2.common;

import com.google.gson.*;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.params.FieldParam;
import com.zilliz.milvustestv2.utils.GenerateUtil;


import com.zilliz.milvustestv2.utils.JsonObjectUtil;
import com.zilliz.milvustestv2.utils.MathUtil;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.common.utils.Float16Utils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static com.zilliz.milvustestv2.common.BaseTest.milvusClientV2;

/**
 * @Author yongpeng.li
 * @Date 2024/2/1 15:55
 */
@Slf4j
public class CommonFunction {
    /**
     * 提供Collection Schema
     *
     * @param dim        维度
     * @param vectorType 向量类型
     * @return CollectionSchema
     */
    public static CreateCollectionReq.CollectionSchema providerCollectionSchema(int dim, DataType vectorType) {
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(100)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(100)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        fieldSchemaList.add(fieldVector);
        return CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
    }

    /**
     * 创建DataType vectorType类型向量的collection
     *
     * @param dim            维度
     * @param collectionName collection name
     * @param vectorType     向量类型-sparse vector 不需要dim
     * @return collection name
     */
    public static String createNewCollection(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(100)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(100)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        log.info("create collection:" + collectionName);
        return collectionName;
    }

    /**
     * Create a new collection with Varchar primary key
     *
     * @param dim            dimension of the vector field
     * @param collectionName collection name
     * @param vectorType     vector data type
     * @return collection name
     */
    public static String createNewCollectionWithVarcharPK(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        // Use Varchar as primary key
        CreateCollectionReq.FieldSchema fieldVarcharPK = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(DataType.VarChar)
                .isPrimaryKey(true)
                .name(CommonData.fieldVarchar)
                .maxLength(100)
                .build();
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(false)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldVarcharPK);
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(false)
                .description("collection with varchar primary key")
                .numShards(1)
                .build();
        milvusClientV2.createCollection(createCollectionReq);
        log.info("create collection with varchar pk:" + collectionName);
        return collectionName;
    }

    public static String createNewCollectionWithDatabase(int dim, String collectionName, DataType vectorType,String databaseName) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(100)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(100)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        fieldSchemaList.add(fieldVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(false)
                .description("collection desc")
                .databaseName(databaseName)
                .numShards(1)
                .build();
        milvusClientV2.createCollection(createCollectionReq);
        log.info("create collection:" + collectionName);
        return collectionName;
    }
    public static String createNewCollectionWithDynamic(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(100)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(100)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        fieldSchemaList.add(fieldVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .enableDynamicField(true)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(true)
                .description("collection desc")
                .numShards(1)
                .build();
        milvusClientV2.createCollection(createCollectionReq);
        log.info("create collection with dynamic field:" + collectionName);
        return collectionName;
    }

    /**
     * 创建包含nullable列的collection
     *
     * @param dim            维度
     * @param collectionName collection name
     * @param vectorType     向量类型-sparse vector 不需要dim
     * @return collection name
     */
    public static String createNewNullableCollection(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        log.info("create collection:" + collectionName);
        return collectionName;
    }

    /**
     * 创建包含default value列的collection
     *
     * @param dim            维度
     * @param collectionName collection name
     * @param vectorType     向量类型-sparse vector 不需要dim
     * @return collection name
     */
    public static String createNewDefaultValueCollection(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueInt)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueDouble)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueBool)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .defaultValue(CommonData.defaultValueString)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueFloat)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        log.info("create collection:" + collectionName);
        return collectionName;
    }

    /**
     * 创建包含同时enable nullable和default value列的collection
     *
     * @param dim            维度
     * @param collectionName collection name
     * @param vectorType     向量类型-sparse vector 不需要dim
     * @return collection name
     */
    public static String createNewNullableDefaultValueCollection(int dim, String collectionName, DataType vectorType) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueInt)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueDouble)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isNullable(true)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueBool)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueString)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueFloat)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isNullable(true)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(vectorType)
                .isPrimaryKey(false)
                .build();
        if (vectorType == DataType.FloatVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloatVector);
        }
        if (vectorType == DataType.BinaryVector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBinaryVector);
        }
        if (vectorType == DataType.Float16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldFloat16Vector);
        }
        if (vectorType == DataType.BFloat16Vector) {
            fieldVector.setDimension(dim);
            fieldVector.setName(CommonData.fieldBF16Vector);
        }
        if (vectorType == DataType.SparseFloatVector) {
            fieldVector.setName(CommonData.fieldSparseVector);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
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
        log.info("create collection:" + collectionName);
        return collectionName;
    }

    /**
     * 为不同类型向量的collection提供导入的数据，目前只支持行式插入
     *
     * @param num 数据量
     * @param dim 维度
     * @return List<JsonObject>
     */
    public static List<JsonObject> generateDefaultData(long startId, long num, int dim, DataType vectorType) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = startId; i < (num + startId); i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            row.addProperty(CommonData.fieldInt32, (int) i % 32767);
            row.addProperty(CommonData.fieldInt16, (int) i % 32767);
            row.addProperty(CommonData.fieldInt8, (short) i % 127);
            row.addProperty(CommonData.fieldDouble, (double) i);
            row.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.addProperty(CommonData.fieldBool, i % 2 == 0);
            row.addProperty(CommonData.fieldVarchar, "Str" + i);
            row.addProperty(CommonData.fieldFloat, (float) i);
            // 判断vectorType
            if (vectorType == DataType.FloatVector) {
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            }
            if (vectorType == DataType.BinaryVector) {
                row.add(CommonData.fieldBinaryVector, gson.toJsonTree(generateBinaryVector(dim).array()));
            }
            if (vectorType == DataType.Float16Vector) {
                row.add(CommonData.fieldFloat16Vector, gson.toJsonTree(generateFloat16Vector(dim).array()));
            }
            if (vectorType == DataType.BFloat16Vector) {
                row.add(CommonData.fieldBF16Vector, gson.toJsonTree(generateBF16Vector(dim).array()));
            }
            if (vectorType == DataType.SparseFloatVector) {
                row.add(CommonData.fieldSparseVector, gson.toJsonTree(generateSparseVector(dim)));
            }

            JsonObject json = new JsonObject();
            json.addProperty(CommonData.fieldInt64, (int) i % 32767);
            json.addProperty(CommonData.fieldInt32, (int) i % 32767);
            json.addProperty(CommonData.fieldDouble, (double) i);
            json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            json.addProperty(CommonData.fieldBool, i % 2 == 0);
            json.addProperty(CommonData.fieldVarchar, "Str" + i);
            json.addProperty(CommonData.fieldFloat, (float) i);
            row.add(CommonData.fieldJson, json);
            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * Generate data with varchar primary key for collection created by createNewCollectionWithVarcharPK
     *
     * @param startId    start id
     * @param num        number of entities to generate
     * @param dim        dimension of vector
     * @param vectorType vector data type
     * @return List of JsonObject representing the data rows
     */
    public static List<JsonObject> generateDataWithVarcharPK(long startId, long num, int dim, DataType vectorType) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = startId; i < (num + startId); i++) {
            JsonObject row = new JsonObject();
            // Use varchar as primary key
            row.addProperty(CommonData.fieldVarchar, "Str" + i);
            row.addProperty(CommonData.fieldInt64, i);
            row.addProperty(CommonData.fieldInt32, (int) i % 32767);
            row.addProperty(CommonData.fieldInt8, (short) i % 127);
            row.addProperty(CommonData.fieldDouble, (double) i);
            row.addProperty(CommonData.fieldBool, i % 2 == 0);
            row.addProperty(CommonData.fieldFloat, (float) i);
            // Generate vector based on type
            if (vectorType == DataType.FloatVector) {
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            }
            if (vectorType == DataType.BinaryVector) {
                row.add(CommonData.fieldBinaryVector, gson.toJsonTree(generateBinaryVector(dim).array()));
            }
            if (vectorType == DataType.Float16Vector) {
                row.add(CommonData.fieldFloat16Vector, gson.toJsonTree(generateFloat16Vector(dim).array()));
            }
            if (vectorType == DataType.BFloat16Vector) {
                row.add(CommonData.fieldBF16Vector, gson.toJsonTree(generateBF16Vector(dim).array()));
            }
            if (vectorType == DataType.SparseFloatVector) {
                row.add(CommonData.fieldSparseVector, gson.toJsonTree(generateSparseVector(dim)));
            }
            JsonObject json = new JsonObject();
            json.addProperty(CommonData.fieldInt64, (int) i % 32767);
            json.addProperty(CommonData.fieldInt32, (int) i % 32767);
            json.addProperty(CommonData.fieldDouble, (double) i);
            json.addProperty(CommonData.fieldFloat, (float) i);
            row.add(CommonData.fieldJson, json);
            jsonList.add(row);
        }
        return jsonList;
    }

    public static List<JsonObject> generateDefaultDataWithDynamic(long startId, long num, int dim, DataType vectorType) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = startId; i < (num + startId); i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            row.addProperty(CommonData.fieldInt32, (int) i % 32767);
            row.addProperty(CommonData.fieldInt16, (int) i % 32767);
            row.addProperty(CommonData.fieldInt8, (short) i % 127);
            row.addProperty(CommonData.fieldDouble, (double) i);
            row.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.addProperty(CommonData.fieldBool, i % 2 == 0);
            row.addProperty(CommonData.fieldVarchar, "Str" + i);
            row.addProperty(CommonData.fieldFloat, (float) i);
            // 判断vectorType
            if (vectorType == DataType.FloatVector) {
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            }
            if (vectorType == DataType.BinaryVector) {
                row.add(CommonData.fieldBinaryVector, gson.toJsonTree(generateBinaryVector(dim).array()));
            }
            if (vectorType == DataType.Float16Vector) {
                row.add(CommonData.fieldFloat16Vector, gson.toJsonTree(generateFloat16Vector(dim).array()));
            }
            if (vectorType == DataType.BFloat16Vector) {
                row.add(CommonData.fieldBF16Vector, gson.toJsonTree(generateBF16Vector(dim).array()));
            }
            if (vectorType == DataType.SparseFloatVector) {
                row.add(CommonData.fieldSparseVector, gson.toJsonTree(generateSparseVector(dim)));
            }

            JsonObject json = new JsonObject();
            json.addProperty(CommonData.fieldInt64, (int) i % 32767);
            json.addProperty(CommonData.fieldInt32, (int) i % 32767);
            json.addProperty(CommonData.fieldDouble, (double) i);
            json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            json.addProperty(CommonData.fieldBool, i % 2 == 0);
            json.addProperty(CommonData.fieldVarchar, "Str" + i);
            json.addProperty(CommonData.fieldFloat, (float) i);
            row.add(CommonData.fieldJson, json);
            // dynamic field
            JsonObject jsonDynamic = new JsonObject();
            json.addProperty(CommonData.fieldInt64, (int) i % 32767);
            json.addProperty(CommonData.fieldInt32, (int) i % 32767);
            json.addProperty(CommonData.fieldDouble, (double) i);
            json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            json.addProperty(CommonData.fieldBool, i % 2 == 0);
            json.addProperty(CommonData.fieldVarchar, "Str" + i);
            json.addProperty(CommonData.fieldFloat, (float) i);
            row.add(CommonData.fieldDynamic, json);

            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * 为快速生成的collection提供导入数据
     *
     * @param num 数据量
     * @param dim 维度
     * @return List<JsonObject>
     */
    public static List<JsonObject> generateSimpleData(long num, int dim) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0; i < num; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.simplePk, i);
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < dim; ++k) {
                vector.add(ran.nextFloat());
            }
            row.add(CommonData.simpleVector, gson.toJsonTree(vector));
            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * 为collection提供导入含有NULL的数据，目前只支持行式插入
     *
     * @param num 数据量
     * @param dim 维度
     * @return List<JsonObject>
     */
    public static List<JsonObject> generateSimpleNullData(long startId, long num, int dim, DataType vectorType) {
        List<JsonObject> jsonList = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = startId; i < (num + startId); i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            if (i % 2 == 0) {
                row.addProperty(CommonData.fieldInt32, (int) i % 32767);
                row.addProperty(CommonData.fieldInt16, (int) i % 32767);
                row.addProperty(CommonData.fieldInt8, (short) i % 127);
                row.addProperty(CommonData.fieldBool, i % 3 == 0);
                row.addProperty(CommonData.fieldDouble, (double) i);
                row.addProperty(CommonData.fieldVarchar, "Str" + i);
                row.addProperty(CommonData.fieldFloat, (float) i);
                row.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            }
            // 判断vectorType
            if (vectorType == DataType.FloatVector) {
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                    vector.add(ran.nextFloat());
                }
                row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            }
            if (vectorType == DataType.BinaryVector) {
                row.add(CommonData.fieldBinaryVector, gson.toJsonTree(generateBinaryVector(dim).array()));
            }
            if (vectorType == DataType.Float16Vector) {
                row.add(CommonData.fieldFloat16Vector, gson.toJsonTree(generateFloat16Vector(dim).array()));
            }
            if (vectorType == DataType.BFloat16Vector) {
                row.add(CommonData.fieldBF16Vector, gson.toJsonTree(generateBF16Vector(dim).array()));
            }
            if (vectorType == DataType.SparseFloatVector) {
                row.add(CommonData.fieldSparseVector, gson.toJsonTree(generateSparseVector(dim)));
            }

            JsonObject json = new JsonObject();
            if (i % 2 == 0) {
                json.addProperty(CommonData.fieldInt64, (int) i % 32767);
                json.addProperty(CommonData.fieldInt32, (int) i % 32767);
                json.addProperty(CommonData.fieldDouble, (double) i);
                json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
                json.addProperty(CommonData.fieldBool, i % 3 == 0);
                json.addProperty(CommonData.fieldVarchar, "Str" + i);
                json.addProperty(CommonData.fieldFloat, (float) i);
            }
            row.add(CommonData.fieldJson, json);
            jsonList.add(row);
        }
        return jsonList;
    }


    /**
     * 快速创建一个collection，只有主键和向量字段
     *
     * @param dim            维度
     * @param collectionName collection name
     * @return collectionName
     */
    public static String createSimpleCollection(int dim, String collectionName, boolean autoPK) {
        if (collectionName == null) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .autoID(autoPK)
                .dimension(dim)
                .enableDynamicField(false)
                .build());
        return collectionName;
    }

    /**
     * 创建索引时，提供额外的参数
     *
     * @param indexType 索引类型
     * @return Map类型参数
     */
    public static Map<String, Object> provideExtraParam(IndexParam.IndexType indexType) {
        Map<String, Object> map = new HashMap<>();
        switch (indexType) {
            case FLAT:
            case AUTOINDEX:
                break;
            case HNSW:
                map.put("M", 16);
                map.put("efConstruction", 64);
                break;
            default:
                map.put("nlist", 128);
                break;
        }
        return map;
    }


    /**
     * 创建向量索引
     *
     * @param collectionName collectionName
     * @param vectorName     向量名称
     * @param indexType      indexType
     * @param metricType     metricType
     */
    public static void createVectorIndex(String collectionName, String vectorName, IndexParam.IndexType indexType, IndexParam.MetricType metricType) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorName)
                .indexType(indexType)
                .extraParams(provideExtraParam(indexType))
                .metricType(metricType)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    /**
     * 创建标量索引
     *
     * @param collectionName collectionName
     * @param scalarName     多个标量名称的集合
     */
    public static void createScalarIndex(String collectionName, List<String> scalarName) {
        List<IndexParam> indexParams = new ArrayList<>();
        scalarName.forEach(x -> {
            IndexParam indexParam = IndexParam.builder().indexType(IndexParam.IndexType.TRIE).fieldName(x).build();
            indexParams.add(indexParam);
        });
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(indexParams)
                .build());
    }


    public static void createPartition(String collectionName, String partitionName) {
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(collectionName)
                .partitionName(partitionName)
                .build());
    }

    public static SearchResp defaultSearch(String collectionName) {
        List<List<Float>> vectors = GenerateUtil.generateFloatVector(10, 3, CommonData.dim);
        List<BaseVector> data = new ArrayList<>();
        vectors.forEach((v) -> {
            data.add(new FloatVec(v));
        });
        return milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(CommonData.topK)
                .build());
    }

    /**
     * 创建一条float32的向量
     *
     * @param dimension 维度
     * @return List<Float>
     */
    public static List<Float> generateFloatVector(int dimension) {
        Random ran = new Random();
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; ++i) {
            vector.add(ran.nextFloat());
        }
        return vector;
    }

    /**
     * 创建一条Sparse向量数据
     *
     * @param dim 维度，sparse不需要指定维度，所以方法里随机
     * @return SortedMap<Long, Float>
     */
    public static SortedMap<Long, Float> generateSparseVector(int dim) {
        Random ran = new Random();
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dimNum = ran.nextInt(dim) + 1;
        for (int i = 0; i < dimNum; ++i) {
            sparse.put((long) ran.nextInt(1000000), ran.nextFloat());
        }
        return sparse;
    }

    /**
     * 创建多条Sparse向量数据
     *
     * @param dim 维度，sparse不需要指定维度，所以方法里随机
     * @return List<SortedMap < Long, Float>>
     */
    public static List<SortedMap<Long, Float>> generateSparseVectors(int dim, long count) {
        List<SortedMap<Long, Float>> list = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            list.add(generateSparseVector(dim));
        }
        return list;
    }

    /**
     * 创建一条float16的向量
     *
     * @param dim 维度
     * @return ByteBuffer
     */
    public static ByteBuffer generateFloat16Vector(int dim) {
        List<Float> originalVector = generateFloatVector(dim);
        return Float16Utils.f32VectorToFp16Buffer(originalVector);
    }

    /**
     * 创建指定数量的float16的向量
     *
     * @param dim   维度
     * @param count 指定条数
     * @return List<ByteBuffer>
     */
    public static List<ByteBuffer> generateFloat16Vectors(int dim, long count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFloat16Vector(dim));
        }
        return vectors;
    }

    /**
     * 创建一条BF16的向量
     *
     * @param dim
     * @return ByteBuffer
     */
    public static ByteBuffer generateBF16Vector(int dim) {
        List<Float> originalVector = generateFloatVector(dim);
        return Float16Utils.f32VectorToBf16Buffer(originalVector);
    }

    /**
     * 创建指定数量的BF16的向量
     *
     * @param dim
     * @param count
     * @return List<ByteBuffer>
     */
    public static List<ByteBuffer> generateBF16Vectors(int dim, long count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBF16Vector(dim));
        }
        return vectors;
    }

    /**
     * 生成一条binary向量
     *
     * @param dim 维度
     * @return ByteBuffer
     */
    public static ByteBuffer generateBinaryVector(int dim) {
        Random ran = new Random();
        int byteCount = dim / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }


    /**
     * 生成指定数量的binary向量数据
     *
     * @param count binary向量的数据条数
     * @param dim   维度
     * @return List<ByteBuffer>
     */
    public static List<ByteBuffer> generateBinaryVectors(int dim, long count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBinaryVector(dim));
        }
        return vectors;
    }

    private static JsonArray toJsonArray(byte[] bytes) {
        JsonArray jsonArray = new JsonArray();
        for (byte b : bytes) {
            jsonArray.add(b);
        }
        return jsonArray;
    }

    /**
     * 创建索引
     *
     * @param collection collection name
     * @param vectorType 向量类型
     */
    public static void createIndex(String collection, DataType vectorType) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(provideFieldVectorName(vectorType))
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(provideMetricTypeByVectorType(vectorType))
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    /**
     * 更具向量类型提供MetricType
     *
     * @param vectorType 向量类型
     * @return MetricType
     */
    public static IndexParam.MetricType provideMetricTypeByVectorType(DataType vectorType) {
        switch (vectorType.getCode()) {
            case 101:
            case 102:
            case 103:
                return IndexParam.MetricType.L2;
            case 100:
                return IndexParam.MetricType.HAMMING;
            case 104:
                return IndexParam.MetricType.IP;
            default:
                return IndexParam.MetricType.INVALID;
        }
    }

    /**
     * 更具向量类型提供向量name
     *
     * @param vectorType 向量类型
     * @return vector field name
     */
    public static String provideFieldVectorName(DataType vectorType) {
        switch (vectorType.getCode()) {
            case 101:
                return CommonData.fieldFloatVector;
            case 102:
                return CommonData.fieldFloat16Vector;
            case 103:
                return CommonData.fieldBF16Vector;
            case 100:
                return CommonData.fieldBinaryVector;
            case 104:
                return CommonData.fieldSparseVector;
            default:
                return "";
        }
    }

    /**
     * collection建索引+insert+load
     *
     * @param collectionName collection name
     * @param vectorType     向量类型
     * @param ifLoad         是否load
     */
    public static void createIndexAndInsertAndLoad(String collectionName, DataType vectorType, @NonNull Boolean ifLoad, Long numberEntities) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(provideFieldVectorName(vectorType))
                .indexType(providerIndexType(vectorType))
                .extraParams(CommonFunction.provideExtraParam(providerIndexType(vectorType)))
                .metricType(provideMetricTypeByVectorType(vectorType))
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        if (ifLoad) {
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
        }
//        insertIntoCollectionByBatch(collectionName, numberEntities, CommonData.dim, vectorType);
        List<JsonObject> jsonObjects = genCommonData(collectionName, numberEntities);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(collectionName).data(jsonObjects).build());


    }

    public static void insertIntoCollectionByBatch(String collectionName, long num, int dim, DataType vectorType) {
        long insertRounds = (num / CommonData.batchSize) == 0 ? 1 : (num / CommonData.batchSize);
        for (int i = 0; i < insertRounds; i++) {
            System.out.println("insert batch:" + (i + 1));
            List<JsonObject> jsonObjects = generateDefaultData(i * CommonData.batchSize, CommonData.batchSize, dim, vectorType);
            InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(collectionName).data(jsonObjects).build());
        }
    }

    /**
     * 提供search时候的向量参数
     *
     * @param nq         向量个数
     * @param dim        维度
     * @param vectorType 向量类型
     * @return List<BaseVector>
     */
    public static List<BaseVector> providerBaseVector(int nq, int dim, DataType vectorType) {
        List<BaseVector> data = new ArrayList<>();
        if (vectorType.equals(DataType.FloatVector)) {
            List<List<Float>> lists = GenerateUtil.generateFloatVector(nq, 3, dim);
            lists.forEach((v) -> {
                data.add(new FloatVec(v));
            });
        }
        if (vectorType.equals(DataType.BinaryVector)) {
            List<ByteBuffer> byteBuffers = generateBinaryVectors(dim, nq);
            byteBuffers.forEach(x -> {
                data.add(new BinaryVec(x));
            });
        }
        if (vectorType.equals(DataType.Float16Vector)) {
            List<ByteBuffer> byteBuffers = generateFloat16Vectors(dim, nq);
            byteBuffers.forEach(x -> {
                data.add(new Float16Vec(x));
            });
        }
        if (vectorType.equals(DataType.BFloat16Vector)) {
            List<ByteBuffer> byteBuffers = generateBF16Vectors(dim, nq);
            byteBuffers.forEach(x -> {
                data.add(new BFloat16Vec(x));
            });
        }
        if (vectorType.equals(DataType.SparseFloatVector)) {
            List<SortedMap<Long, Float>> list = generateSparseVectors(dim, nq);
            list.forEach(x -> {
                data.add(new SparseFloatVec(x));
            });
        }
        return data;

    }

    /**
     * 根据向量类型决定IndexType
     *
     * @param vectorType DataType
     * @return IndexParam.IndexType
     */
    public static IndexParam.IndexType providerIndexType(DataType vectorType) {
        switch (vectorType.getCode()) {
            case 101:
                return IndexParam.IndexType.HNSW;
            case 102:
                return IndexParam.IndexType.HNSW;
            case 103:
                return IndexParam.IndexType.HNSW;
            case 100:
                return IndexParam.IndexType.BIN_IVF_FLAT;
            case 104:
                return IndexParam.IndexType.SPARSE_WAND;
            default:
                return IndexParam.IndexType.TRIE;
        }
    }


    /**
     * 创建通用的collection方法，支持多个filed，多个向量
     *
     * @param collectionName collection 可不传
     * @param pkDataType     主键类型
     * @param enableDynamic  是否开启动态列
     * @param fieldParamList 其他字段
     * @return collection name
     */
    public static String genCommonCollection(@Nullable String collectionName, DataType pkDataType, boolean enableDynamic, List<FieldParam> fieldParamList) {
        if (collectionName == null || collectionName.equals("")) {
            collectionName = "Collection_" + GenerateUtil.getRandomString(10);
        }
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = parseDataType(fieldParamList);
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(pkDataType)
                .isPrimaryKey(true)
                .name(pkDataType + "_0")
                .build();
        fieldSchemaList.add(fieldInt64);


        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionName)
                .enableDynamicField(enableDynamic)
                .description("collection desc")
                .numShards(1)
                .build();
        milvusClientV2.createCollection(createCollectionReq);
        return collectionName;
    }

    /**
     * 遍历fieldParamList生成对应的schema
     *
     * @param fieldParamList field字段集合
     * @return List<CreateCollectionReq.FieldSchema> 给创建collection提供
     */
    public static List<CreateCollectionReq.FieldSchema> parseDataType(List<FieldParam> fieldParamList) {
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        for (FieldParam fieldParam : fieldParamList) {
            //按照_分组
            DataType dataType = fieldParam.getDataType();
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .dataType(dataType)
                    .name(fieldParam.getFieldName())
                    .isPrimaryKey(false)
                    .build();
            if (dataType == DataType.FloatVector || dataType == DataType.BFloat16Vector || dataType == DataType.Float16Vector || dataType == DataType.BinaryVector) {
                fieldSchema.setDimension(fieldParam.getDim());
            }
            if (dataType == DataType.String || dataType == DataType.VarChar) {
                fieldSchema.setMaxLength(fieldParam.getMaxLength());
            }
            if (dataType == DataType.Array) {
                fieldSchema.setMaxCapacity(fieldParam.getMaxCapacity());
                fieldSchema.setElementType(fieldParam.getElementType());
            }
            fieldSchemaList.add(fieldSchema);
        }
        return fieldSchemaList;
    }


    /**
     * 生成通用的数据
     *
     * @param collectionName 向量名称
     * @param count          生成的数量
     * @return List<JsonObject>
     */
    public static List<JsonObject> genCommonData(String collectionName, long count) {
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder().collectionName(collectionName).build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = collectionSchema.getFieldSchemaList();
        List<JsonObject> jsonList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
                String name = fieldSchema.getName();
                DataType dataType = fieldSchema.getDataType();
                Integer dimension = fieldSchema.getDimension();
                Integer maxCapacity = fieldSchema.getMaxCapacity();
                Integer maxLength = fieldSchema.getMaxLength();
                JsonObject jsonObject;
                if (dataType == DataType.FloatVector || dataType == DataType.BFloat16Vector || dataType == DataType.Float16Vector || dataType == DataType.BinaryVector) {
                    jsonObject = generalJsonObjectByDataType(name, dataType, dimension, i);
                } else if (dataType == DataType.SparseFloatVector) {
                    jsonObject = generalJsonObjectByDataType(name, dataType, CommonData.dim, i);
                } else if (dataType == DataType.VarChar || dataType == DataType.String) {
                    jsonObject = generalJsonObjectByDataType(name, dataType, maxLength, i);
                } else if (dataType == DataType.Array) {
                    jsonObject = generalJsonObjectByDataType(name, dataType, maxCapacity, i);
                } else {
                    jsonObject = generalJsonObjectByDataType(name, dataType, 0, i);
                }
                row = JsonObjectUtil.jsonMerge(row, jsonObject);
            }
            // 判断是否有动态列
            if (describeCollectionResp.getCollectionSchema().isEnableDynamicField()) {
                JsonObject jsonObject = generalJsonObjectByDataType(CommonData.dynamicField, DataType.JSON, 0, i);
                row = JsonObjectUtil.jsonMerge(row, jsonObject);
            }
            jsonList.add(row);
        }
        return jsonList;
    }

    /**
     * 更具数据类型，创建JsonObject
     *
     * @param fieldName   字段名称
     * @param dataType    类型
     * @param dimOrLength 向量维度或者array容量或者varchar长度
     * @param countIndex  索引i，避免多次创建时数据内容一样
     * @return JsonObject
     */
    public static JsonObject generalJsonObjectByDataType(String fieldName, DataType dataType, int dimOrLength, long countIndex) {
        JsonObject row = new JsonObject();
        Gson gson = new Gson();
        Random random = new Random();
        if (dataType == DataType.Int64) {
            row.addProperty(fieldName, countIndex);
        }
        if (dataType == DataType.Int32) {
            row.addProperty(fieldName, (int) countIndex % 32767);
        }
        if (dataType == DataType.Int16) {
            row.addProperty(fieldName, (int) countIndex % 32767);
        }
        if (dataType == DataType.Int8) {
            row.addProperty(fieldName, (short) countIndex % 127);
        }
        if (dataType == DataType.Double) {
            row.addProperty(fieldName, (double) countIndex * 0.1f);
        }
        if (dataType == DataType.Array) {
            int i = random.nextInt(dimOrLength);
            List<Long> arrays = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                arrays.add(countIndex + j);
            }
            row.add(fieldName, gson.toJsonTree(arrays));
        }
        if (dataType == DataType.Bool) {
            row.addProperty(fieldName, countIndex % 2 == 0);
        }
        if (dataType == DataType.VarChar) {
            int i = random.nextInt(dimOrLength / 2);
            String s = MathUtil.genRandomStringAndChinese(i);
            row.addProperty(fieldName, s);
        }
        if (dataType == DataType.String) {
            int i = random.nextInt(dimOrLength / 2);
            String s = MathUtil.genRandomStringAndChinese(i);
            row.addProperty(fieldName, s);
        }
        if (dataType == DataType.Float) {
            row.addProperty(fieldName, (float) countIndex * 0.1f);
        }
        if (dataType == DataType.FloatVector) {
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < dimOrLength; ++k) {
                vector.add(random.nextFloat());
            }
            row.add(fieldName, gson.toJsonTree(vector));
        }
        if (dataType == DataType.BinaryVector) {
            row.add(fieldName, gson.toJsonTree(generateBinaryVector(dimOrLength).array()));
        }
        if (dataType == DataType.Float16Vector) {
            row.add(fieldName, gson.toJsonTree(generateFloat16Vector(dimOrLength).array()));
        }
        if (dataType == DataType.BFloat16Vector) {
            row.add(fieldName, gson.toJsonTree(generateBF16Vector(dimOrLength).array()));
        }
        if (dataType == DataType.SparseFloatVector) {
            row.add(fieldName, gson.toJsonTree(generateSparseVector(dimOrLength)));
        }
        if (dataType == DataType.JSON) {
            JsonObject json = new JsonObject();
            json.addProperty(CommonData.fieldInt64, (int) countIndex % 32767);
            json.addProperty(CommonData.fieldInt32, (int) countIndex % 32767);
            json.addProperty(CommonData.fieldDouble, (double) countIndex);
            json.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(countIndex, countIndex + 1, countIndex + 2)));
            json.addProperty(CommonData.fieldBool, countIndex % 2 == 0);
            json.addProperty(CommonData.fieldVarchar, "Str" + countIndex);
            json.addProperty(CommonData.fieldFloat, (float) countIndex);
            row.add(fieldName, json);
        }
        return row;
    }

    /**
     * 创建通用索引
     *
     * @param collection     collection name
     * @param fieldParamList field集合
     */
    public static void createCommonIndex(String collection, List<FieldParam> fieldParamList) {
        List<IndexParam> indexParamList = new ArrayList<>();
        for (FieldParam fieldParam : fieldParamList) {
            //按照_分组
            DataType dataType = fieldParam.getDataType();
            String fieldName = fieldParam.getFieldName();

            IndexParam indexParam = IndexParam.builder()
                    .fieldName(fieldName)
                    .indexType(providerIndexType(dataType))
                    .extraParams(CommonFunction.provideExtraParam(providerIndexType(dataType)))
                    .metricType(provideMetricTypeByVectorType(dataType))
                    .build();
            indexParamList.add(indexParam);
        }

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(indexParamList)
                .build());
    }

    /**
     * Create Scalar Indexes
     *
     * @param collection     collection name
     * @param fieldParamList scalar fields
     */
    public static void createScalarCommonIndex(String collection, List<FieldParam> fieldParamList) {
        List<IndexParam> indexParamList = new ArrayList<>();
        for (FieldParam fieldParam : fieldParamList) {
            IndexParam.IndexType indexType = fieldParam.getIndextype();
            String fieldName = fieldParam.getFieldName();

            IndexParam indexParam = IndexParam.builder()
                    .fieldName(fieldName)
                    .indexType(indexType)
                    .indexName(fieldName)
                    .build();
            indexParamList.add(indexParam);
        }

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(indexParamList)
                .build());
    }

    /**
     * Drop Scalar Indexes
     *
     * @param collection     collection name
     * @param fieldParamList FieldParamList
     */
    public static void dropScalarCommonIndex(String collection, List<FieldParam> fieldParamList) {
        List<String> fieldNames = fieldParamList.stream().map(FieldParam::getFieldName).collect(Collectors.toList());
        fieldNames.forEach(x -> milvusClientV2.dropIndex(DropIndexReq.builder()
                .collectionName(collection)
                .fieldName(x)
                .indexName(x)
                .build()));
    }


    /**
     * 为多向量查询提供AnnSearch
     *
     * @param fieldParam 字段参数
     * @param nq         传入的向量数
     * @param topK       查询数量
     * @param expr       表达式
     * @return AnnSearchReq
     */
    public static AnnSearchReq provideAnnSearch(FieldParam fieldParam, int nq, int topK, String expr) {
        DataType dataType = fieldParam.getDataType();
        int dim = fieldParam.getDim();
        List<BaseVector> baseVectors = providerBaseVector(nq, dim, dataType);
        return AnnSearchReq.builder().vectors(baseVectors)
                .topK(topK)
                .vectorFieldName(fieldParam.getFieldName())
                .params(provideSearchParam(providerIndexType(dataType)))
                .expr(expr).build();
    }

    /**
     * 根据索引类型提供查询参数
     *
     * @param indexType index type
     * @return String 查询参数
     */
    public static String provideSearchParam(IndexParam.IndexType indexType) {
        String extraParam;
        switch (indexType) {
            case FLAT:
                extraParam = "{}";
                break;
            case IVF_FLAT:
                extraParam = "{\"nlist\":32,\"nprobe\":32}";
                break;
            case IVF_SQ8:
                extraParam = "{\"nlist\":128}";
                break;
            case IVF_PQ:
                extraParam = "{\"nlist\":128, \"m\":16, \"nbits\":8}";
                break;
            case HNSW:
                extraParam = "{\"M\":16,\"efConstruction\":64}";
                break;
            case BIN_IVF_FLAT:
                extraParam = "{\"nlist\": 128}";
                break;
            case SCANN:
                extraParam = "{\"nlist\":1024,\"with_raw_data\":" + true + "}";
                break;
            case GPU_IVF_FLAT:
                extraParam = "{\"nlist\": 64}";
                break;
            case GPU_IVF_PQ:
                extraParam = "{\"nlist\": 64, \"m\": 16, \"nbits\": 8}";
                break;
            case SPARSE_INVERTED_INDEX:
            case SPARSE_WAND:
                extraParam = "{\"drop_ratio_search\":0.2}";
                break;
            default:
                extraParam = "{\"nlist\":128}";
                break;
        }
        return extraParam;
    }

    /**
     * 提供bulk import时候的files
     *
     * @param collection   collection
     * @param bulkFileType 文件类型--枚举类bulkFileType
     * @return
     */
    public static List<List<String>> providerBatchFiles(String collection, BulkFileType bulkFileType, long count) {
        // 查询schema
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder().collectionName(collection).build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        RemoteBulkWriter remoteBulkWriter = buildRemoteBulkWriter(collectionSchema, bulkFileType);
        List<JsonObject> jsonObjects = CommonFunction.genCommonData(collection, count);
        jsonObjects.forEach(x -> {
            try {
                remoteBulkWriter.appendRow(x);
            } catch (IOException | InterruptedException e) {
                log.error(e.getMessage());
            }
        });
        System.out.printf("%s rows appends%n", remoteBulkWriter.getTotalRowCount());
        System.out.printf("%s rows in buffer not flushed%n", remoteBulkWriter.getTotalRowCount());
        try {
            remoteBulkWriter.commit(false);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
        List<List<String>> batchFiles = remoteBulkWriter.getBatchFiles();
        System.out.printf("Remote writer done! output remote files: %s%n", batchFiles);
        return batchFiles;
    }

    /**
     * 为开源提供 remote bulk writer
     * RemoteBulkWriterParam = LocalBulkWriterParam + uploadObject + clearData
     *
     * @param collectionSchema
     * @param bulkFileType
     * @return
     */
    private static RemoteBulkWriter buildRemoteBulkWriter(CreateCollectionReq.CollectionSchema collectionSchema, BulkFileType bulkFileType) {
        StorageConnectParam connectParam = S3ConnectParam.newBuilder()
                .withEndpoint(System.getProperty("minio") == null ? PropertyFilesUtil.getRunValue("minio") : System.getProperty("minio"))
                .withCloudName(CloudStorage.MINIO.getCloudName())
                .withBucketName("milvus-bucket")
                .withAccessKey("minioadmin")
                .withSecretKey("minioadmin")
                .withRegion("")
                .build();
        RemoteBulkWriterParam bulkWriterParam = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withRemotePath("bulk_data")
                .withFileType(bulkFileType)
                .withChunkSize(5 * 1024 * 1024 * 1024L)
                .withConnectParam(connectParam)
                .build();
        RemoteBulkWriter remoteBulkWriter = null;
        try {
            remoteBulkWriter = new RemoteBulkWriter(bulkWriterParam);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return remoteBulkWriter;
    }


    private static LocalBulkWriter buildLocalBulkWriter(CreateCollectionReq.CollectionSchema collectionSchema, BulkFileType bulkFileType) {
        LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(bulkFileType)
                .withChunkSize(5 * 1024 * 1024 * 1024L)
                .build();
        LocalBulkWriter localBulkWriter;
        try {
            localBulkWriter = new LocalBulkWriter(bulkWriterParam);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return localBulkWriter;
    }

    public static List<List<String>> providerLocalBatchFiles(String collection, BulkFileType bulkFileType, long count) {
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder().collectionName(collection).build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        LocalBulkWriter localBulkWriter = buildLocalBulkWriter(collectionSchema, bulkFileType);
        List<JsonObject> jsonObjects = CommonFunction.genCommonData(collection, count);
        for (JsonObject jsonObject : jsonObjects) {
            try {
                localBulkWriter.appendRow(jsonObject);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
        System.out.printf("%s rows in buffer not flushed%n", localBulkWriter.getTotalRowCount());

        try {
            localBulkWriter.commit(false);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<List<String>> batchFiles = localBulkWriter.getBatchFiles();
        System.out.printf("Local writer done! output remote files: %s%n", batchFiles);
        return batchFiles;
    }

    // minio上传--copy from v1
    public static void multiFilesUpload(String path, List<List<String>> batchFiles) {

        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint(System.getProperty("minio") == null ? PropertyFilesUtil.getRunValue("minio") : System.getProperty("minio"))
                        .credentials("minioadmin", "minioadmin")
                        .build();
        // Make 'jsonBucket' bucket if not exist.
        boolean found = false;
        try {
            found = minioClient.bucketExists(BucketExistsArgs.builder().bucket("milvus-bucket").build());
            if (!found) {
                // Make a new bucket called 'jsonBucket'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket("milvus-bucket").build());
            } else {
                System.out.println("Bucket 'milvus-bucket' already exists.");
            }

            List<String> fileNameList=new ArrayList<>();
            for (List<String> batchFileList : batchFiles) {
                fileNameList.addAll(batchFileList);
            }
            for (String fileName : fileNameList) {
                minioClient.uploadObject(
                        UploadObjectArgs.builder()
                                .bucket("milvus-bucket")
                                .object( fileName)
                                .filename( fileName)
                                .build());
                System.out.println(
                        "'"
                                + path
                                + fileName
                                + "' is successfully uploaded as "
                                + "object '"
                                + fileName
                                + "' to bucket 'milvus-bucket'.");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    // ==================== Struct Array Related Methods ====================

    /**
     * Create a collection schema with Struct field containing vectors
     *
     * @param collectionName collection name
     * @param dim            vector dimension
     * @return collection name
     */
    public static String createStructCollection(String collectionName, int dim) {
        if (collectionName == null || collectionName.isEmpty()) {
            collectionName = "StructCollection_" + GenerateUtil.getRandomString(10);
        }

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();

        // Primary key field
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        // Regular float vector field
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(dim)
                .build());

        // Struct array field with multiple sub-fields including vectors
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldStruct)
                .description("struct array field with vectors")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(CommonData.structMaxCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName(CommonData.structFieldInt32)
                        .description("int32 field in struct")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(CommonData.structFieldVarchar)
                        .description("varchar field in struct")
                        .dataType(DataType.VarChar)
                        .maxLength(1024)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(CommonData.structFieldFloatVector1)
                        .description("first float vector in struct")
                        .dataType(DataType.FloatVector)
                        .dimension(dim)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(CommonData.structFieldFloatVector2)
                        .description("second float vector in struct")
                        .dataType(DataType.FloatVector)
                        .dimension(dim)
                        .build())
                .build());

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(collectionSchema)
                .enableDynamicField(false)
                .numShards(1)
                .build();

        milvusClientV2.createCollection(createCollectionReq);
        log.info("Created struct collection: " + collectionName);
        return collectionName;
    }

    /**
     * Generate data for struct collection
     *
     * @param startId start id
     * @param count   number of rows
     * @param dim     vector dimension
     * @return list of JsonObject data
     */
    public static List<JsonObject> generateStructData(long startId, long count, int dim) {
        List<JsonObject> dataList = new ArrayList<>();
        Random random = new Random();

        for (long i = startId; i < startId + count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);

            // Regular float vector
            List<Float> vector = GenerateUtil.generateFloatVector(1, 6, dim).get(0);
            row.add(CommonData.fieldFloatVector, new com.google.gson.Gson().toJsonTree(vector));

            // Struct array - each row has 3-10 struct elements
            int structCount = random.nextInt(8) + 3;
            JsonArray structArray = new JsonArray();
            for (int j = 0; j < structCount; j++) {
                JsonObject structElement = new JsonObject();
                structElement.addProperty(CommonData.structFieldInt32, random.nextInt(10000));
                structElement.addProperty(CommonData.structFieldVarchar, "struct_desc_" + i + "_" + j);

                // First vector in struct
                List<Float> vec1 = GenerateUtil.generateFloatVector(1, 6, dim).get(0);
                structElement.add(CommonData.structFieldFloatVector1, new com.google.gson.Gson().toJsonTree(vec1));

                // Second vector in struct
                List<Float> vec2 = GenerateUtil.generateFloatVector(1, 6, dim).get(0);
                structElement.add(CommonData.structFieldFloatVector2, new com.google.gson.Gson().toJsonTree(vec2));

                structArray.add(structElement);
            }
            row.add(CommonData.fieldStruct, structArray);

            dataList.add(row);
        }
        return dataList;
    }

    /**
     * Create embedding list index for struct vector field
     *
     * @param collectionName collection name
     * @param structFieldName struct field name
     * @param vectorFieldName vector field name in struct
     * @param indexName       index name
     * @param metricType      metric type (MAX_SIM_COSINE, MAX_SIM_IP, MAX_SIM_L2)
     */
    public static void createStructVectorIndex(String collectionName, String structFieldName,
                                               String vectorFieldName, String indexName,
                                               IndexParam.MetricType metricType) {
        String fullFieldName = String.format("%s[%s]", structFieldName, vectorFieldName);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(fullFieldName)
                .indexName(indexName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(metricType)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 200);
                }})
                .build();

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        log.info("Created struct vector index: " + indexName + " on " + fullFieldName);
    }

    /**
     * Generate EmbeddingList from struct query result
     *
     * @param structData struct field data from query result
     * @param vectorFieldName vector field name in struct
     * @return EmbeddingList
     */
    public static EmbeddingList generateEmbeddingListFromStruct(List<Map<String, Object>> structData,
                                                                 String vectorFieldName) {
        EmbeddingList embeddingList = new EmbeddingList();
        for (Map<String, Object> struct : structData) {
            @SuppressWarnings("unchecked")
            List<Float> vector = (List<Float>) struct.get(vectorFieldName);
            embeddingList.add(new FloatVec(vector));
        }
        return embeddingList;
    }

    /**
     * Generate random EmbeddingList for search
     *
     * @param vectorCount number of vectors in embedding list
     * @param dim         vector dimension
     * @return EmbeddingList
     */
    public static EmbeddingList generateRandomEmbeddingList(int vectorCount, int dim) {
        EmbeddingList embeddingList = new EmbeddingList();
        for (int i = 0; i < vectorCount; i++) {
            List<Float> vector = GenerateUtil.generateFloatVector(1, 6, dim).get(0);
            embeddingList.add(new FloatVec(vector));
        }
        return embeddingList;
    }

}


