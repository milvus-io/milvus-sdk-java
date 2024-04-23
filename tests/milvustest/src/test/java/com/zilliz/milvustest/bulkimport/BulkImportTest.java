package com.zilliz.milvustest.bulkimport;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseCloudTest;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.bulkwriter.CloudImport;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ImportUtils;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.bulkwriter.response.BulkImportResponse;
import io.milvus.bulkwriter.response.GetImportProgressResponse;
import io.milvus.common.utils.ExceptionUtils;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.param.R;
import io.milvus.param.collection.*;
import io.milvus.response.GetCollStatResponseWrapper;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @Author xuan.qi
 * @Date 2024/3/21 17:34
 */
public class BulkImportTest extends BaseCloudTest {
    String ALL_TYPES_COLLECTION_NAME = "all_types_for_bulkwriter";

    @AfterClass
    public void clearTestData(){
        milvusCloudClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(ALL_TYPES_COLLECTION_NAME).build());
    }

    private CollectionSchemaParam buildAllTypeSchema() {
        // scalar field
        FieldType fieldType1 = FieldType.newBuilder()
                .withName("id")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();

        FieldType fieldType2 = FieldType.newBuilder()
                .withName("bool")
                .withDataType(DataType.Bool)
                .build();

        FieldType fieldType3 = FieldType.newBuilder()
                .withName("int8")
                .withDataType(DataType.Int8)
                .build();

        FieldType fieldType4 = FieldType.newBuilder()
                .withName("int16")
                .withDataType(DataType.Int16)
                .build();

        FieldType fieldType5 = FieldType.newBuilder()
                .withName("int32")
                .withDataType(DataType.Int32)
                .build();

        FieldType fieldType6 = FieldType.newBuilder()
                .withName("float")
                .withDataType(DataType.Float)
                .build();

        FieldType fieldType7 = FieldType.newBuilder()
                .withName("double")
                .withDataType(DataType.Double)
                .build();

        FieldType fieldType8 = FieldType.newBuilder()
                .withName("varchar")
                .withDataType(DataType.VarChar)
                .withMaxLength(512)
                .build();

        FieldType fieldType9 = FieldType.newBuilder()
                .withName("json")
                .withDataType(DataType.JSON)
                .build();

        // vector field
        FieldType fieldType10;
        if (false) {
            fieldType10 = FieldType.newBuilder()
                    .withName("vector")
                    .withDataType(DataType.BinaryVector)
                    .withDimension(128)
                    .build();
        } else {
            fieldType10 = FieldType.newBuilder()
                    .withName("vector")
                    .withDataType(DataType.FloatVector)
                    .withDimension(128)
                    .build();
        }

        CollectionSchemaParam.Builder schemaBuilder = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(false)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .addFieldType(fieldType3)
                .addFieldType(fieldType4)
                .addFieldType(fieldType5)
                .addFieldType(fieldType6)
                .addFieldType(fieldType7)
                .addFieldType(fieldType8)
                .addFieldType(fieldType9)
                .addFieldType(fieldType10);

        // array field
        if (true) {
            FieldType fieldType11 = FieldType.newBuilder()
                    .withName("arrayInt64")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Int64)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType12 = FieldType.newBuilder()
                    .withName("arrayVarchar")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.VarChar)
                    .withMaxLength(10)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType13 = FieldType.newBuilder()
                    .withName("arrayInt8")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Int8)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType14 = FieldType.newBuilder()
                    .withName("arrayInt16")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Int16)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType15 = FieldType.newBuilder()
                    .withName("arrayInt32")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Int32)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType16 = FieldType.newBuilder()
                    .withName("arrayFloat")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Float)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType17 = FieldType.newBuilder()
                    .withName("arrayDouble")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Double)
                    .withMaxCapacity(10)
                    .build();

            FieldType fieldType18 = FieldType.newBuilder()
                    .withName("arrayBool")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Bool)
                    .withMaxCapacity(10)
                    .build();

            schemaBuilder.addFieldType(fieldType11)
                    .addFieldType(fieldType12)
                    .addFieldType(fieldType13)
                    .addFieldType(fieldType14)
                    .addFieldType(fieldType15)
                    .addFieldType(fieldType16)
                    .addFieldType(fieldType17)
                    .addFieldType(fieldType18);
        }
        return schemaBuilder.build();
    }

    private RemoteBulkWriter buildRemoteBulkWriter(CollectionSchemaParam collectionSchema, BulkFileType fileType) throws IOException {
        StorageConnectParam connectParam = buildStorageConnectParam();
        RemoteBulkWriterParam bulkWriterParam = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withRemotePath("bulk_data")
                .withFileType(fileType)
                .withChunkSize(512 * 1024 * 1024)
                .withConnectParam(connectParam)
                .build();
        return new RemoteBulkWriter(bulkWriterParam);
    }

    private static StorageConnectParam buildStorageConnectParam() {
        StorageConnectParam connectParam;
        if (Objects.equals(PropertyFilesUtil.getRunValue("storageType"), "azure")) {
            String connectionStr = "DefaultEndpointsProtocol=https;AccountName=" + PropertyFilesUtil.getRunValue("azureAccountName") +
                    ";AccountKey=" + PropertyFilesUtil.getRunValue("azureAccountKey") + ";EndpointSuffix=core.windows.net";
            connectParam = AzureConnectParam.newBuilder()
                    .withConnStr(connectionStr)
                    .withContainerName(PropertyFilesUtil.getRunValue("azureContainerName"))
                    .build();
        } else {
            // only for aws
            CloudStorage cloudStorage = CloudStorage.AWS;
            connectParam = S3ConnectParam.newBuilder()
                    .withEndpoint(cloudStorage.getEndpoint())
                    .withBucketName(PropertyFilesUtil.getRunValue("storageBucket"))
                    .withAccessKey(PropertyFilesUtil.getRunValue("storageAccessKey"))
                    .withSecretKey(PropertyFilesUtil.getRunValue("storageSecretKey"))
                    .withRegion(PropertyFilesUtil.getRunValue("storageRegion"))
                    .build();
        }
        return connectParam;
    }

    private List<List<String>> allTypesRemoteWriter(CollectionSchemaParam collectionSchema) throws Exception {
        System.out.printf("\n===================== all field types (%s) binary_vector=%s ====================%n", BulkFileType.PARQUET.name(), false);

        try (RemoteBulkWriter remoteBulkWriter = buildRemoteBulkWriter(collectionSchema, BulkFileType.PARQUET)) {
            System.out.println("Append rows");
            int batchCount = 10;

            for (int i = 0; i < batchCount; ++i) {
                JSONObject rowObject = new JSONObject();

                // scalar field
                rowObject.put("id", i);
                rowObject.put("bool", i % 5 == 0);
                rowObject.put("int8", i % 128);
                rowObject.put("int16", i % 1000);
                rowObject.put("int32", i % 100000);
                rowObject.put("float", i / 3);
                rowObject.put("double", i / 7);
                rowObject.put("varchar", "varchar_" + i);
                rowObject.put("json", String.format("{\"dummy\": %s, \"ok\": \"name_%s\"}", i, i));

                // vector field
                rowObject.put("vector", false ? GeneratorUtils.generatorBinaryVector(128) : GeneratorUtils.generatorFloatValue(128));

                // array field
                rowObject.put("arrayInt64", GeneratorUtils.generatorLongValue(10));
                rowObject.put("arrayVarchar", GeneratorUtils.generatorVarcharValue(10, 10));
                rowObject.put("arrayInt8", GeneratorUtils.generatorInt8Value(10));
                rowObject.put("arrayInt16", GeneratorUtils.generatorInt16Value(10));
                rowObject.put("arrayInt32", GeneratorUtils.generatorInt32Value(10));
                rowObject.put("arrayFloat", GeneratorUtils.generatorFloatValue(10));
                rowObject.put("arrayDouble", GeneratorUtils.generatorDoubleValue(10));
                rowObject.put("arrayBool", GeneratorUtils.generatorBoolValue(10));

                remoteBulkWriter.appendRow(rowObject);
            }
            System.out.printf("%s rows appends%n", remoteBulkWriter.getTotalRowCount());
            System.out.printf("%s rows in buffer not flushed%n", remoteBulkWriter.getBufferRowCount());
            System.out.println("Generate data files...");
            remoteBulkWriter.commit(false);

            System.out.printf("Data files have been uploaded: %s%n", remoteBulkWriter.getBatchFiles());
            return remoteBulkWriter.getBatchFiles();
        } catch (Exception e) {
            System.out.println("allTypesRemoteWriter catch exception: " + e);
            throw e;
        }
    }

    private void createCollection(String collectionName, CollectionSchemaParam collectionSchema, boolean dropIfExist) {
        System.out.println("\n===================== create collection ====================");
        CreateCollectionParam collectionParam = CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSchema(collectionSchema)
                .build();
        R<Boolean> hasCollection = milvusCloudClient.hasCollection(HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
        if (hasCollection.getData()) {
            if (dropIfExist) {
                milvusCloudClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
                milvusCloudClient.createCollection(collectionParam);
            }
        } else {
            milvusCloudClient.createCollection(collectionParam);
        }
        System.out.printf("Collection %s created%n", collectionName);
    }

    @Test(description = "bulk remote import test")
    public void bulkRemoteImportTest() throws Exception {
        String cloudEndpoint = PropertyFilesUtil.getRunValue("cloudEndpoint");
        String apiKey = PropertyFilesUtil.getRunValue("apikey");
        String clusterId = PropertyFilesUtil.getRunValue("clusterId");
        CollectionSchemaParam collectionSchema = buildAllTypeSchema();
        List<List<String>> batchFiles = allTypesRemoteWriter(collectionSchema);
        createCollection(ALL_TYPES_COLLECTION_NAME, collectionSchema, false);
        System.out.println("\n===================== call cloudImport ====================");
        CloudStorage cloudStorage = CloudStorage.AZURE;
        if (Objects.equals(PropertyFilesUtil.getRunValue("storageType"), "aws")) {
            cloudStorage = CloudStorage.AWS;
        }
        String objectUrl = cloudStorage == CloudStorage.AZURE
                ? cloudStorage.getAzureObjectUrl(PropertyFilesUtil.getRunValue("azureAccountName"), PropertyFilesUtil.getRunValue("azureContainerName"), ImportUtils.getCommonPrefix(batchFiles))
                : cloudStorage.getS3ObjectUrl(PropertyFilesUtil.getRunValue("storageBucket"), ImportUtils.getCommonPrefix(batchFiles), PropertyFilesUtil.getRunValue("storageRegion"));
        String accessKey = cloudStorage == CloudStorage.AZURE ? PropertyFilesUtil.getRunValue("azureAccountName") : PropertyFilesUtil.getRunValue("storageAccessKey");
        String secretKey = cloudStorage == CloudStorage.AZURE ? PropertyFilesUtil.getRunValue("azureAccountKey") : PropertyFilesUtil.getRunValue("storageSecretKey");

        BulkImportResponse bulkImportResponse = CloudImport.bulkImport(cloudEndpoint, apiKey, objectUrl, accessKey,
                secretKey, clusterId, ALL_TYPES_COLLECTION_NAME);
        String jobId = bulkImportResponse.getJobId();
        System.out.println("Create a cloudImport job, job id: " + jobId);

        while (true) {
            System.out.println("Wait 5 second to check bulkInsert job state...");
            TimeUnit.SECONDS.sleep(5);

            GetImportProgressResponse getImportProgressResponse = CloudImport.getImportProgress(cloudEndpoint, apiKey, jobId, clusterId);
            if (getImportProgressResponse.getReadyPercentage().intValue() == 1) {
                System.out.printf("The job %s completed%n", jobId);
                break;
            } else if (StringUtils.isNotEmpty(getImportProgressResponse.getErrorMessage())) {
                System.out.printf("The job %s failed, reason: %s%n", jobId, getImportProgressResponse.getErrorMessage());
                break;
            } else {
                System.out.printf("The job %s is running, progress:%s%n", jobId, getImportProgressResponse.getReadyPercentage());
            }
        }
        milvusCloudClient.flush(FlushParam.newBuilder().addCollectionName(ALL_TYPES_COLLECTION_NAME).build());
        R<GetCollectionStatisticsResponse> response = milvusCloudClient.getCollectionStatistics(
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName(ALL_TYPES_COLLECTION_NAME)
                        .build());
        ExceptionUtils.handleResponseStatus(response);
        GetCollStatResponseWrapper wrapper = new GetCollStatResponseWrapper(response.getData());
        Assert.assertEquals(wrapper.getRowCount(), 10);
        System.out.println("Collection row number: " + wrapper.getRowCount());
    }


}