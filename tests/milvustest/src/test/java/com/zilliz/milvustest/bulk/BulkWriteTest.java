package com.zilliz.milvustest.bulk;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.ImportResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.R;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.ShowCollectionsParam;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @Author yongpeng.li
 * @Date 2024/3/14 17:34
 */
public class BulkWriteTest extends BaseTest {
    String newCollection="";
    CollectionSchemaParam collectionSchemaParam;
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        newCollection = CommonFunction.createNewCollectionWithJSONField();
        collectionSchemaParam = CommonFunction.provideJsonCollectionSchema();
    }

    @AfterClass(alwaysRun = true)
    public void clearTestData(){
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(newCollection).build());
    }

    @Test(description = "bulk write test",groups = {"Smoke"})
    public void bulkWriteTest() throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeyException {
        LocalBulkWriterParam localBulkWriterParam= LocalBulkWriterParam.newBuilder()
                .withFileType(BulkFileType.PARQUET)
                .withLocalPath("./src/test/java/resources/temp/bulkWrite")
                .withChunkSize(10 * 1024 * 1024) //10MB
                .withCollectionSchema(collectionSchemaParam)
                .build();
        LocalBulkWriter localBulkWriter=new LocalBulkWriter(localBulkWriterParam);

        List<JSONObject> jsonObjects = CommonFunction.generateJsonData(10000);
        jsonObjects.forEach(x->{
            try {
                localBulkWriter.appendRow(x);
            } catch (IOException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        });
        System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
        System.out.printf("%s rows in buffer not flushed%n", localBulkWriter.getBufferRowCount());
        localBulkWriter.commit(false);
        System.out.println(localBulkWriter.getBatchFiles().toString());

        List<List<String>> batchFiles = localBulkWriter.getBatchFiles();
        List<String> fileLists=new ArrayList<>();
        batchFiles.forEach(x-> fileLists.add(x.get(0).substring(x.get(0).lastIndexOf("/")+1)));
        String path=batchFiles.get(0).get(0).substring(0,batchFiles.get(0).get(0).lastIndexOf("/")+1);
        R<GetCollectionStatisticsResponse> collectionStatistics = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                .withCollectionName(newCollection)
                .withFlush(true)
                .build());
        Assert.assertEquals(collectionStatistics.getData().getStats(0).getValue(),"0");
        // bulk insert
        // 上传
        FileUtils.multiFilesUpload(path, fileLists, null);
        // bulkinsert
        BulkInsertParam build = BulkInsertParam.newBuilder()
                .withCollectionName(newCollection)
                .addFile(fileLists.get(0))
                .build();
        R<ImportResponse> importResponseR = milvusClient.bulkInsert(build);
        Thread.sleep(20000L);
        R<GetCollectionStatisticsResponse> collectionStatistics2 = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                .withCollectionName(newCollection)
                .withFlush(true)
                .build());
        Assert.assertEquals(collectionStatistics2.getData().getStats(0).getValue(), "10000");
    }

    @Test(description = "bulk remote write test")
    public void bulkRemoteWriteTest() throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeyException {
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
        RemoteBulkWriterParam bulkWriterParam = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchemaParam)
                .withRemotePath("bulk_data")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(512 * 1024 * 1024)
                .withConnectParam(connectParam)
                .build();
        try (RemoteBulkWriter remoteBulkWriter = new RemoteBulkWriter(bulkWriterParam)) {
            // append rows
            for (int i = 0; i < 10; i++) {
                JSONObject row = new JSONObject();
                row.put("string_field", "path_" + i);
                row.put("float_vector", GeneratorUtils.genFloatVector(128));
                row.put("int64_field", (long)i);
                row.put("boolean_field", false);
                row.put("json_field", new JSONObject());
                row.put("float_field", (float)i);

                remoteBulkWriter.appendRow(row);
            }

            System.out.printf("%s rows appends%n", remoteBulkWriter.getTotalRowCount());
            System.out.printf("%s rows in buffer not flushed%n", remoteBulkWriter.getBufferRowCount());

            remoteBulkWriter.commit(false);
            List<List<String>> batchFiles = remoteBulkWriter.getBatchFiles();
            Assert.assertEquals(batchFiles.size(), 1);
            System.out.printf("Remote writer done! output remote files: %s%n", batchFiles);
        } catch (Exception e) {
            System.out.println("remoteWriter catch exception: " + e);
        }
    }

}