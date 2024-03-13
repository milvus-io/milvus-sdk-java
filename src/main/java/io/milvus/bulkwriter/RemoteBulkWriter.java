package io.milvus.bulkwriter;

import com.alibaba.fastjson.JSONObject;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.Lists;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.bulkwriter.storage.client.AzureStorageClient;
import io.milvus.bulkwriter.storage.client.MinioStorageClient;
import io.milvus.common.utils.ExceptionUtils;
import io.minio.errors.ErrorResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RemoteBulkWriter extends LocalBulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBulkWriter.class);

    private String remotePath;
    private StorageConnectParam connectParam;
    private StorageClient storageClient;

    private List<List<String>> remoteFiles;

    public RemoteBulkWriter(RemoteBulkWriterParam bulkWriterParam) throws IOException {
        super(bulkWriterParam.getCollectionSchema(), bulkWriterParam.getChunkSize(), bulkWriterParam.getFileType(), generatorLocalPath());
        Path path = Paths.get(bulkWriterParam.getRemotePath());
        Path remoteDirPath = path.resolve(getUUID());
        this.remotePath = remoteDirPath.toString();
        this.connectParam = bulkWriterParam.getConnectParam();
        getStorageClient();

        this.remoteFiles = Lists.newArrayList();
        logger.info("Remote buffer writer initialized, target path: {}", remotePath);

    }

    @Override
    public void appendRow(JSONObject rowData) throws IOException, InterruptedException {
        super.appendRow(rowData);
    }

    @Override
    public void commit(boolean async) throws InterruptedException {
        super.commit(async);
    }

    @Override
    protected String getDataPath() {
        return remotePath;
    }

    @Override
    public List<List<String>> getBatchFiles() {
        return remoteFiles;
    }

    @Override
    protected void exit() throws InterruptedException {
        super.exit();
        // remove the temp folder "bulk_writer"
        Path parentPath = Paths.get(localPath).getParent();
        if (parentPath.toFile().exists() && isEmptyDirectory(parentPath)) {
            try {
                Files.delete(parentPath);
                logger.info("Delete empty directory: " + parentPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isEmptyDirectory(Path directory) {
        try {
            return !Files.walk(directory, 1, FileVisitOption.FOLLOW_LINKS)
                    .skip(1) // Skip the root directory itself
                    .findFirst()
                    .isPresent();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void getStorageClient() {
        if (storageClient != null) {
            return;
        }

        if (connectParam instanceof S3ConnectParam) {
            S3ConnectParam s3ConnectParam = (S3ConnectParam) connectParam;
            storageClient = MinioStorageClient.getStorageClient(s3ConnectParam.getEndpoint(),
                    s3ConnectParam.getAccessKey(),
                    s3ConnectParam.getSecretKey(),
                    s3ConnectParam.getSessionToken(),
                    s3ConnectParam.getRegion(),
                    s3ConnectParam.getHttpClient());
        } else if (connectParam instanceof AzureConnectParam) {
            AzureConnectParam azureConnectParam = (AzureConnectParam) connectParam;
            storageClient = AzureStorageClient.getStorageClient(azureConnectParam.getConnStr(),
                    azureConnectParam.getAccountUrl(),
                    azureConnectParam.getCredential());
        }
    }

    private void rmLocal(String file) {
        try {
            Path filePath = Paths.get(file);
            filePath.toFile().delete();

            Path parentDir = filePath.getParent();
            if (parentDir != null && !parentDir.toString().equals(localPath)) {
                try {
                    Files.delete(parentDir);
                    logger.info("Delete empty directory: " + parentDir);
                } catch (IOException ex) {
                    logger.warn("Failed to delete empty directory: " + parentDir);
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to delete local file: " + file);
        }
    }

    @Override
    protected void callBack(List<String> fileList) {
        List<String> remoteFileList = new ArrayList<>();
        try {
            if (!bucketExists()) {
                ExceptionUtils.throwUnExpectedException("Blob storage bucket/container doesn't exist");
            }

            for (String filePath : fileList) {
                String ext = getExtension(filePath);
                if (!Lists.newArrayList(".parquet").contains(ext)) {
                    continue;
                }

                String relativeFilePath = filePath.replace(super.getDataPath(), "");
                String minioFilePath = getMinioFilePath(remotePath, relativeFilePath);

                if (objectExists(minioFilePath)) {
                    logger.info(String.format("Remote file %s already exists, will overwrite it", minioFilePath));
                }
                uploadObject(filePath, minioFilePath);
                remoteFileList.add(minioFilePath);
                rmLocal(filePath);
            }

        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to upload files, error: %s", e));
        }

        logger.info("Successfully upload files: " + fileList);
        remoteFiles.add(remoteFileList);
    }

    @Override
    public void close() throws Exception {
        logger.info("execute remaining actions to prevent loss of memory data or residual empty directories.");
        exit();
        logger.info(String.format("RemoteBulkWriter done! output remote files: %s", getBatchFiles()));
    }

    private void getObjectEntity(String objectName) throws Exception {
        if (connectParam instanceof S3ConnectParam) {
            S3ConnectParam s3ConnectParam = (S3ConnectParam) connectParam;
            storageClient.getObjectEntity(s3ConnectParam.getBucketName(), objectName);
        } else if (connectParam instanceof AzureConnectParam) {
            AzureConnectParam azureConnectParam = (AzureConnectParam) connectParam;
            storageClient.getObjectEntity(azureConnectParam.getContainerName(), objectName);
        }

        ExceptionUtils.throwUnExpectedException("Blob storage client is not initialized");
    }

    private boolean objectExists(String objectName) throws Exception {
        try {
            getObjectEntity(objectName);
        } catch (ErrorResponseException e) {
            if ("NoSuchKey".equals(e.errorResponse().code())) {
                return false;
            }

            String msg = String.format("Failed to stat MinIO/S3 object %s, error: %s", objectName, e.errorResponse().message());
            ExceptionUtils.throwUnExpectedException(msg);
        } catch (BlobStorageException e) {
            if (BlobErrorCode.BLOB_NOT_FOUND == e.getErrorCode()) {
                return false;
            }
            String msg = String.format("Failed to stat Azure object %s, error: %s", objectName, e.getServiceMessage());
            ExceptionUtils.throwUnExpectedException(msg);
        }
        return true;
    }

    private boolean bucketExists() throws Exception {
        if (connectParam instanceof S3ConnectParam) {
            S3ConnectParam s3ConnectParam = (S3ConnectParam) connectParam;
            return storageClient.checkBucketExist(s3ConnectParam.getBucketName());
        } else if (connectParam instanceof AzureConnectParam) {
            AzureConnectParam azureConnectParam = (AzureConnectParam) connectParam;
            return storageClient.checkBucketExist(azureConnectParam.getContainerName());
        }

        ExceptionUtils.throwUnExpectedException("Blob storage client is not initialized");
        return false;
    }

    private void uploadObject(String filePath, String objectName) throws Exception {
        logger.info(String.format("Prepare to upload %s to %s", filePath, objectName));

        File file = new File(filePath);
        FileInputStream fileInputStream = new FileInputStream(file);
        if (connectParam instanceof S3ConnectParam) {
            S3ConnectParam s3ConnectParam = (S3ConnectParam) connectParam;
            storageClient.putObjectStream(fileInputStream, file.length(), s3ConnectParam.getBucketName(), objectName);
        } else if (connectParam instanceof AzureConnectParam) {
            AzureConnectParam azureConnectParam = (AzureConnectParam) connectParam;
            storageClient.putObjectStream(fileInputStream, file.length(), azureConnectParam.getContainerName(), objectName);
        } else {
            ExceptionUtils.throwUnExpectedException("Blob storage client is not initialized");
        }
        logger.info(String.format("Upload file %s to %s", filePath, objectName));
    }


    private static String generatorLocalPath() {
        Path currentWorkingDirectory = Paths.get("").toAbsolutePath();
        Path currentScriptPath = currentWorkingDirectory.resolve("bulk_writer");
        return currentScriptPath.toString();
    }

    private static String getMinioFilePath(String remotePath, String relativeFilePath) {
        remotePath = remotePath.startsWith("/") ? remotePath.substring(1) : remotePath;
        Path remote = Paths.get(remotePath);

        relativeFilePath = relativeFilePath.startsWith("/") ? relativeFilePath.substring(1) : relativeFilePath;
        Path relative = Paths.get(relativeFilePath);
        Path joinedPath = remote.resolve(relative);
        return joinedPath.toString();
    }

    private static String getExtension(String filePath) {
        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');

        if (dotIndex == -1 || dotIndex == fileName.length() - 1) {
            return "";
        } else {
            return fileName.substring(dotIndex);
        }
    }
}
