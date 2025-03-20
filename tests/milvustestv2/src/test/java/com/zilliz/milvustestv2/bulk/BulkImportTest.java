package com.zilliz.milvustestv2.bulk;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.bulkwriter.BulkImport;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.request.describe.MilvusDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.response.GetResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class BulkImportTest extends BaseTest {

    String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = CommonFunction.createSimpleCollection(CommonData.dim, null, false);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName).build());
    }

    @Test(description = "remote bulk import", groups = {"L1"})
    public void remoteBulkImport() {
        List<List<String>> batchFiles = CommonFunction.providerBatchFiles(collectionName, BulkFileType.PARQUET, CommonData.numberEntities);
        MilvusImportRequest milvusImportRequest = MilvusImportRequest.builder()
                .collectionName(collectionName)
                .files(batchFiles)
                .build();
        String bulkImportResult = BulkImport.bulkImport(
                System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                milvusImportRequest);
        JsonObject bulkImportJO = JsonParser.parseString(bulkImportResult).getAsJsonObject();
        String jobId = bulkImportJO.getAsJsonObject("data").get("jobId").getAsString();
        System.out.println(jobId);
        Assert.assertNotNull(jobId);
        // 遍历导入进程
        String status = "";
        int i = 0;
        while (!status.equalsIgnoreCase("Completed") && i < 10) {

            MilvusDescribeImportRequest request = MilvusDescribeImportRequest.builder()
                    .jobId(jobId)
                    .build();
            String importProgress = BulkImport.getImportProgress(System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                    request);
            JsonObject jsonObject = JsonParser.parseString(importProgress).getAsJsonObject();
            status = jsonObject.getAsJsonObject("data").get("state").getAsString();
            i++;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        GetCollectionStatsResp collectionStats = milvusClientV2.getCollectionStats(GetCollectionStatsReq.builder().collectionName(collectionName).build());
        System.out.println(collectionStats);

    }

    @Test(description = "local bulk import", groups = {"L2"})
    public void localBulkImport() {
        List<List<String>> batchFiles = CommonFunction.providerLocalBatchFiles(collectionName, BulkFileType.PARQUET, CommonData.numberEntities);
        // 上传minio
        CommonFunction.multiFilesUpload("", batchFiles);

        MilvusImportRequest milvusImportRequest = MilvusImportRequest.builder()
                .collectionName(collectionName)
                .files(batchFiles)
                .build();
        String bulkImportResult = BulkImport.bulkImport(
                System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                milvusImportRequest);
        JsonObject bulkImportJO = JsonParser.parseString(bulkImportResult).getAsJsonObject();
        String jobId = bulkImportJO.getAsJsonObject("data").get("jobId").getAsString();
        System.out.println(jobId);
        Assert.assertNotNull(jobId);
        // 遍历导入进程
        String status = "";
        int i = 0;
        while (!status.equalsIgnoreCase("Completed") && i < 10) {

            MilvusDescribeImportRequest request = MilvusDescribeImportRequest.builder()
                    .jobId(jobId)
                    .build();
            String importProgress = BulkImport.getImportProgress(System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                    request);
            JsonObject jsonObject = JsonParser.parseString(importProgress).getAsJsonObject();
            status = jsonObject.getAsJsonObject("data").get("state").getAsString();
            i++;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        GetCollectionStatsResp collectionStats = milvusClientV2.getCollectionStats(GetCollectionStatsReq.builder().collectionName(collectionName).build());
        Assert.assertTrue(collectionStats.getNumOfEntities() == CommonData.numberEntities);

    }


}
