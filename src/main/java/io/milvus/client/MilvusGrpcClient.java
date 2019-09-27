package io.milvus.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import io.milvus.client.grpc.RowRecord;
import io.milvus.client.grpc.VectorIds;
import io.milvus.client.params.*;
import org.javatuples.Pair;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MilvusGrpcClient {

    private static final Logger logger = Logger.getLogger(MilvusGrpcClient.class.getName());

    private final ManagedChannel channel;
    private final io.milvus.client.grpc.MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final io.milvus.client.grpc.MilvusServiceGrpc.MilvusServiceFutureStub futureStub;
    private final io.milvus.client.grpc.MilvusServiceGrpc.MilvusServiceStub asyncStub;

    public MilvusGrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public MilvusGrpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = io.milvus.client.grpc.MilvusServiceGrpc.newBlockingStub(channel);
        futureStub = io.milvus.client.grpc.MilvusServiceGrpc.newFutureStub(channel);
        asyncStub = io.milvus.client.grpc.MilvusServiceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        logInfo("Shut down complete");
    }

    ///////////////////////Client Calls///////////////////////

    public Response createTable(@Nonnull TableSchema tableSchema) {

        io.milvus.client.grpc.TableSchema request = io.milvus.client.grpc.TableSchema
                                                     .newBuilder()
                                                     .setTableName(tableSchema.getTableName())
                                                     .setDimension(tableSchema.getDimension())
                                                     .setIndexFileSize(tableSchema.getIndexFileSize())
                                                     .setMetricType(tableSchema.getMetricType().getVal())
                                                     .build();

        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub.createTable(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Created table successfully!\nTableSchema = {0}", tableSchema.toString());
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Create table failed\nTableSchema = {0}", tableSchema.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("Create table RPC failed: {0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    public boolean hasTable(@Nonnull String tableName) {
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                 .newBuilder()
                                                 .setTableName(tableName)
                                                 .build();
        io.milvus.client.grpc.BoolReply response;

        try {
            response = blockingStub.hasTable(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("hasTable \'{0}\' = {1}", tableName, response.getBoolReply());
                return response.getBoolReply();
            } else {
                logSevere("hasTable failed: ", response.toString());
                return false;
            }
        } catch (StatusRuntimeException e) {
            logSevere("hasTable RPC failed: {0}", e.getStatus().toString());
            return false;
        }
    }

    public Response dropTable(@Nonnull String tableName) {
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub.dropTable(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Dropped table \'{0}\' successfully!", tableName);
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Drop table \'{0}\' failed", tableName);
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("Drop table RPC failed: {0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    public Response createIndex(@Nonnull IndexParam indexParam) {
        io.milvus.client.grpc.Index index = io.milvus.client.grpc.Index
                                            .newBuilder()
                                            .setIndexType(indexParam.getIndex().getIndexType().getVal())
                                            .setNlist(indexParam.getIndex().getnNList())
                                            .build();
        io.milvus.client.grpc.IndexParam request = io.milvus.client.grpc.IndexParam
                                           .newBuilder()
                                           .setTableName(indexParam.getTableName())
                                           .setIndex(index)
                                           .build();

        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub.createIndex(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Created index successfully!\nIndexParam = {0}", indexParam.toString());
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Create index failed\nIndexParam = {0}", indexParam.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("Create index RPC failed: {0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    public Pair<Response, Optional<List<Long>>> insert(@Nonnull InsertParam insertParam) {

        List<io.milvus.client.grpc.RowRecord> rowRecordList = new ArrayList<>();
        for (List<Float> vectors : insertParam.getVectors()) {
            io.milvus.client.grpc.RowRecord rowRecord = io.milvus.client.grpc.RowRecord
                                                        .newBuilder()
                                                        .addAllVectorData(vectors)
                                                        .build();
            rowRecordList.add(rowRecord);
        }

        io.milvus.client.grpc.InsertParam request = io.milvus.client.grpc.InsertParam
                                                    .newBuilder()
                                                    .setTableName(insertParam.getTableName())
                                                    .addAllRowRecordArray(rowRecordList)
                                                    .addAllRowIdArray(insertParam.getVectorIds())
                                                    .build();
        io.milvus.client.grpc.VectorIds response;

        try {
            response = blockingStub.insert(request);
            Optional<List<Long>> resultVectorIds = Optional.ofNullable(response.getVectorIdArrayList());

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Inserted vectors successfully!");
                return Pair.with(new Response(Response.Status.SUCCESS), resultVectorIds);
            } else {
                logSevere("Insert vectors failed");
                return Pair.with(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                              response.getStatus().getReason()),
                                 Optional.empty());
            }
        } catch (StatusRuntimeException e) {
            logSevere("Insert RPC failed: {0}", e.getStatus().toString());
            return Pair.with(new Response(Response.Status.RPC_ERROR, e.toString()), Optional.empty());
        }
    }

    /////////////////////Util Functions/////////////////////

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private void logInfo(String msg, Object... params) {
        logger.log(Level.INFO, ANSI_GREEN + msg + ANSI_RESET, params);
    }

    private void logWarning(String msg, Object... params) {
        logger.log(Level.WARNING, ANSI_YELLOW + msg + ANSI_RESET, params);
    }

    private void logSevere(String msg, Object... params) {
        logger.log(Level.SEVERE, ANSI_PURPLE + msg + ANSI_RESET, params);
    }

    //////////////////////////Main///////////////////////////
    public static void main(String[] args) throws InterruptedException {
        MilvusGrpcClient client = new MilvusGrpcClient("192.168.1.188", 19531);

        try {
            String tableName = "test_zhiru";
            long dimension = 128;
            TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                                                               .setIndexFileSize(1024)
                                                               .setMetricType(MetricType.L2)
                                                               .build();
            Response createTableResponse = client.createTable(tableSchema);
            System.out.println(createTableResponse);

            boolean hasTableResponse = client.hasTable(tableName);

            Random random = new Random();
            List<List<Float>> vectors = new ArrayList<>();
            int size = 100;
            for (int i = 0; i < size; ++i) {
                List<Float> vector = new ArrayList<>();
                for (int j = 0; j < dimension; ++j) {
                    vector.add(random.nextFloat());
                }
                vectors.add(vector);
            }
            InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
            Pair<Response, Optional<List<Long>>> insertResult = client.insert(insertParam);
            System.out.println(insertResult.getValue0());

            Index index = new Index.Builder()
                                   .setIndexType(IndexType.IVF_SQ8)
                                   .setNList(16384)
                                   .build();
            IndexParam indexParam = new IndexParam.Builder(tableName)
                                                  .setIndex(index)
                                                  .build();
            Response createIndexResponse = client.createIndex(indexParam);
            System.out.println(createIndexResponse);

        } finally {
            client.shutdown();
        }
    }
}
