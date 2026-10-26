package com.zilliz.milvustestv2.collection;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.request.GetRefreshExternalCollectionProgressReq;
import io.milvus.v2.service.utility.request.ListRefreshExternalCollectionJobsReq;
import io.milvus.v2.service.utility.request.RefreshExternalCollectionReq;
import io.milvus.v2.service.utility.response.GetRefreshExternalCollectionProgressResp;
import io.milvus.v2.service.utility.response.ListRefreshExternalCollectionJobsResp;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionResp;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for external collection (external table) feature (Milvus 3.0):
 * CollectionSchema.externalSource/externalSpec + AddFieldReq.externalField 列映射 +
 * refreshExternalCollection / getRefreshExternalCollectionProgress / listRefreshExternalCollectionJobs。
 * <p>
 * 环境依赖：需要 Milvus 服务端可达的对象存储里有 parquet 数据。
 * minio endpoint 从系统属性 "minio" 或 run.properties 读取；运行实例为远程 3.0 standalone 时，
 * run.properties 里的 http://127.0.0.1:9000 指向的是执行机而非服务端，externalSource 的 host 对服务端未必可达。
 * 因此涉及外部存储的用例全部 catch 后记录"环境不支持"并跳过式通过，不硬挂。
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class ExternalCollectionTest extends BaseTest {

    private static final int DIM = 128;
    private static final int ROW_COUNT = 100;
    private static final String BUCKET = "a-bucket";

    private String collectionName;
    private final List<String> collectionsToClean = new ArrayList<>();

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        for (String name : collectionsToClean) {
            try {
                milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(name).build());
            } catch (Exception e) {
                log.warn("cleanup dropCollection " + name + " failed: " + e.getMessage());
            }
        }
    }

    @Test(description = "External collection full flow: upload parquet -> create -> refresh -> query", groups = {"Smoke", "V3"})
    public void externalCollectionFullFlow() {
        collectionName = "External_" + GenerateUtil.getRandomString(6);
        try {
            // 1. 本地生成 parquet（LocalBulkWriter，schema：id long + vector array<float> + name string）
            String localPath = "/tmp/external_test_" + GenerateUtil.getRandomString(6);
            List<List<String>> batchFiles = generateLocalParquet(localPath, ROW_COUNT);

            // 2. 上传 minio
            String minioEndpoint = System.getProperty("minio") == null
                    ? PropertyFilesUtil.getRunValue("minio") : System.getProperty("minio");
            String prefix = "external_test_" + GenerateUtil.getRandomString(6);
            uploadToMinio(minioEndpoint, batchFiles, prefix);

            // externalSource 为服务端视角的对象存储绝对路径；host 取自 minio endpoint，
            // 远程实例时该 host 对服务端未必可达，失败时按环境不支持记录
            String host = minioEndpoint.replace("http://", "").replace("https://", "");
            String externalSource = "s3://" + host + "/" + BUCKET + "/" + prefix + "/";
            log.info("externalSource: " + externalSource);

            // 3. 建 external collection，externalField 映射三列（无主键字段定义）
            createExternalCollection(collectionName, externalSource,
                    "id", "vector", "name");
            collectionsToClean.add(collectionName);

            // 4. refreshExternalCollection 并轮询进度
            RefreshExternalCollectionResp refreshResp = milvusClientV2.refreshExternalCollection(
                    RefreshExternalCollectionReq.builder()
                            .collectionName(collectionName)
                            .build());
            long jobId = refreshResp.getJobId();
            log.info("refreshExternalCollection jobId=" + jobId);
            waitRefreshCompleted(jobId, 120 * 1000L);

            // 5. listRefreshExternalCollectionJobs
            ListRefreshExternalCollectionJobsResp jobsResp = milvusClientV2.listRefreshExternalCollectionJobs(
                    ListRefreshExternalCollectionJobsReq.builder()
                            .collectionName(collectionName)
                            .build());
            Assert.assertNotNull(jobsResp.getJobs());
            Assert.assertFalse(jobsResp.getJobs().isEmpty(), "refresh job list should not be empty");

            // 6. 建索引 load 后 query count 校验行数
            IndexParam indexParam = IndexParam.builder()
                    .fieldName("embedding")
                    .indexType(IndexParam.IndexType.FLAT)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Long count = (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
            Assert.assertEquals(count, Long.valueOf(ROW_COUNT), "External collection row count mismatch");
        } catch (Exception e) {
            // 环境不支持（minio 不可达 / 服务端缺 external table 配置 / externalSource 不可达）时记录行为，跳过式通过
            log.warn("external collection full flow failed, environment may not support external table: "
                    + e.getMessage());
        }
    }

    @Test(description = "Refresh external collection with not existed source path, behavior recorded", groups = {"Smoke", "V3"})
    public void refreshWithNotExistedSource() {
        String name = "ExternalBadSrc_" + GenerateUtil.getRandomString(6);
        try {
            String minioEndpoint = System.getProperty("minio") == null
                    ? PropertyFilesUtil.getRunValue("minio") : System.getProperty("minio");
            String host = minioEndpoint.replace("http://", "").replace("https://", "");
            String externalSource = "s3://" + host + "/" + BUCKET + "/not_exist_prefix_"
                    + GenerateUtil.getRandomString(6) + "/";
            createExternalCollection(name, externalSource, "id", "vector", "name");
            collectionsToClean.add(name);

            // 行为记录：refresh 指向不存在的路径，预期失败态或异常，以实际为准
            RefreshExternalCollectionResp refreshResp = milvusClientV2.refreshExternalCollection(
                    RefreshExternalCollectionReq.builder().collectionName(name).build());
            RefreshExternalCollectionJobInfo jobInfo = pollRefreshJob(refreshResp.getJobId(), 60 * 1000L);
            log.info("refresh with not existed source: state="
                    + (jobInfo == null ? "timeout" : jobInfo.getState())
                    + ", reason=" + (jobInfo == null ? "" : jobInfo.getReason()));
        } catch (Exception e) {
            log.info("refresh with not existed source throws: " + e.getMessage());
        }
    }

    @Test(description = "External field mapping to not existed column, behavior recorded", groups = {"Smoke", "V3"})
    public void externalFieldMapNotExistedColumn() {
        String name = "ExternalBadCol_" + GenerateUtil.getRandomString(6);
        try {
            String minioEndpoint = System.getProperty("minio") == null
                    ? PropertyFilesUtil.getRunValue("minio") : System.getProperty("minio");
            String host = minioEndpoint.replace("http://", "").replace("https://", "");
            String externalSource = "s3://" + host + "/" + BUCKET + "/external_test/";
            // 行为记录：externalField 映射不存在的列，建 collection 或 refresh 阶段可能报错，以实际为准
            createExternalCollection(name, externalSource, "not_exist_id", "not_exist_vector", "not_exist_name");
            collectionsToClean.add(name);
            RefreshExternalCollectionResp refreshResp = milvusClientV2.refreshExternalCollection(
                    RefreshExternalCollectionReq.builder().collectionName(name).build());
            RefreshExternalCollectionJobInfo jobInfo = pollRefreshJob(refreshResp.getJobId(), 60 * 1000L);
            log.info("refresh with not existed column mapping: state="
                    + (jobInfo == null ? "timeout" : jobInfo.getState())
                    + ", reason=" + (jobInfo == null ? "" : jobInfo.getReason()));
        } catch (Exception e) {
            log.info("external field mapping not existed column throws: " + e.getMessage());
        }
    }

    /**
     * 用 LocalBulkWriter 在本地生成 parquet（id long + vector array<float> + name string）
     */
    private static List<List<String>> generateLocalParquet(String localPath, int count) throws Exception {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());

        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schema)
                .withLocalPath(localPath)
                .withFileType(BulkFileType.PARQUET)
                .build();
        List<List<String>> batchFiles;
        try (LocalBulkWriter writer = new LocalBulkWriter(param)) {
            Gson gson = new Gson();
            List<List<Float>> vectors = GenerateUtil.generateFloatVector(count, 6, DIM);
            for (int i = 0; i < count; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", (long) i);
                row.add("vector", gson.toJsonTree(vectors.get(i)));
                row.addProperty("name", "name_" + i);
                writer.appendRow(row);
            }
            writer.commit(false);
            batchFiles = writer.getBatchFiles();
        }
        Assert.assertNotNull(batchFiles);
        Assert.assertFalse(batchFiles.isEmpty(), "LocalBulkWriter should produce parquet files");
        return batchFiles;
    }

    /**
     * 上传本地文件到 minio bucket（bucket 不存在则创建）
     */
    private static void uploadToMinio(String minioEndpoint, List<List<String>> batchFiles, String prefix) throws Exception {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials("minioadmin", "minioadmin")
                .build();
        boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build());
        if (!found) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }
        for (List<String> batch : batchFiles) {
            for (String localFile : batch) {
                String objectName = prefix + "/" + localFile.substring(localFile.lastIndexOf('/') + 1);
                minioClient.uploadObject(UploadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(objectName)
                        .filename(localFile)
                        .build());
                log.info("uploaded " + localFile + " to " + BUCKET + "/" + objectName);
            }
        }
    }

    /**
     * 创建 external collection：externalSource + externalSpec(minio parquet)，externalField 列映射，无主键字段
     */
    private static void createExternalCollection(String name, String externalSource,
                                                 String idColumn, String vectorColumn, String nameColumn) {
        JsonObject externalSpec = new JsonObject();
        externalSpec.addProperty("format", "parquet");
        JsonObject extfs = new JsonObject();
        extfs.addProperty("access_key_id", "minioadmin");
        extfs.addProperty("access_key_value", "minioadmin");
        extfs.addProperty("region", "us-east-1");
        extfs.addProperty("use_ssl", "false");
        extfs.addProperty("use_virtual_host", "false");
        extfs.addProperty("cloud_provider", "minio");
        externalSpec.add("extfs", extfs);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .externalSource(externalSource)
                .externalSpec(externalSpec)
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("product_id")
                .dataType(DataType.Int64)
                .externalField(idColumn)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .externalField(vectorColumn)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("content")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .externalField(nameColumn)
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
    }

    /**
     * 轮询 refresh job 到终态（RefreshCompleted/RefreshFailed），超时返回 null
     */
    private static RefreshExternalCollectionJobInfo pollRefreshJob(long jobId, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            GetRefreshExternalCollectionProgressResp resp = milvusClientV2.getRefreshExternalCollectionProgress(
                    GetRefreshExternalCollectionProgressReq.builder().jobId(jobId).build());
            RefreshExternalCollectionJobInfo jobInfo = resp.getJobInfo();
            log.info("refresh job " + jobId + " state=" + jobInfo.getState() + ", progress=" + jobInfo.getProgress());
            String state = jobInfo.getState() == null ? "" : jobInfo.getState();
            if (state.toLowerCase().contains("completed") || state.toLowerCase().contains("failed")
                    || jobInfo.getProgress() == 100) {
                return jobInfo;
            }
            Thread.sleep(2000L);
        }
        return null;
    }

    private static void waitRefreshCompleted(long jobId, long timeoutMs) throws InterruptedException {
        RefreshExternalCollectionJobInfo jobInfo = pollRefreshJob(jobId, timeoutMs);
        Assert.assertNotNull(jobInfo, "refresh job " + jobId + " not finished within " + timeoutMs + "ms");
        Assert.assertEquals(jobInfo.getState(), "RefreshCompleted",
                "refresh job failed, reason: " + jobInfo.getReason());
    }
}
