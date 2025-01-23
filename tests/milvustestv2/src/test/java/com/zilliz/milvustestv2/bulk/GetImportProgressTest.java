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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class GetImportProgressTest extends BaseTest {
    String collectionName;
    String jobId;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = CommonFunction.createSimpleCollection(CommonData.dim, null, false);
        List<List<String>> batchFiles = CommonFunction.providerBatchFiles(collectionName, BulkFileType.PARQUET, 10000);

        MilvusImportRequest milvusImportRequest = MilvusImportRequest.builder()
                .collectionName(collectionName)
                .files(batchFiles)
                .build();
        String bulkImportResult = BulkImport.bulkImport(
                System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                milvusImportRequest);
        JsonObject bulkImportJO = JsonParser.parseString(bulkImportResult).getAsJsonObject();
        jobId = bulkImportJO.getAsJsonObject("data").get("jobId").getAsString();
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName).build());
    }

    @Test(description = "get import progress", groups = {"Smoke"})
    public void getImportProgress() {
        MilvusDescribeImportRequest request = MilvusDescribeImportRequest.builder()
                .jobId(jobId)
                .build();
        String importProgress = BulkImport.getImportProgress(System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"),
                request);
        JsonObject jsonObject = JsonParser.parseString(importProgress).getAsJsonObject();
        Assert.assertEquals(jsonObject.get("code").getAsInt(), 0);
    }
}
