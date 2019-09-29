package io.milvus.client;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.Rule;
import org.apache.commons.text.RandomStringGenerator;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MilvusGrpcClientTest {

    @Rule
//    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
//
//    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

    private MilvusGrpcClient client;

    RandomStringGenerator generator;

    private String randomTableName;
    private long size;
    private long dimension;
    private TableParam tableParam;
    private TableSchemaParam tableSchemaParam;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws Exception {
//TODO: dummy service

//        // Generate a unique in-process server name.
//        String serverName = InProcessServerBuilder.generateName();
//        // Use a mutable service registry for later registering the service impl for each test case.
//        grpcCleanup.register(InProcessServerBuilder.forName(serverName)
//                .fallbackHandlerRegistry(serviceRegistry).directExecutor().build().start());
//        client = new MilvusGrpcClient(InProcessChannelBuilder.forName(serverName).directExecutor());

        client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                                        .withHost("192.168.1.188")
                                        .withPort("19530")
                                        .build();
        client.connect(connectParam);

        generator = new RandomStringGenerator.Builder()
                                            .withinRange('a', 'z').build();
        randomTableName = generator.generate(10);
        size = 100;
        dimension = 128;
        tableParam = new TableParam.Builder(randomTableName).build();
        TableSchema tableSchema = new TableSchema.Builder(randomTableName, dimension)
                                                    .withIndexFileSize(1024)
                                                    .withMetricType(MetricType.L2)
                                                    .build();
        tableSchemaParam = new TableSchemaParam.Builder(tableSchema).build();

        assertTrue(client.createTable(tableSchemaParam).ok());
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws InterruptedException {
        assertTrue(client.dropTable(tableParam).ok());
        client.disconnect();
    }

    @org.junit.jupiter.api.Test
    void connected() {
        assertTrue(client.connected());
    }

    @org.junit.jupiter.api.Test
    void createTable() {
        String invalidTableName = "╯°□°）╯";
        TableSchema invalidTableSchema = new TableSchema.Builder(invalidTableName, dimension).build();
        TableSchemaParam invalidTableSchemaParam = new TableSchemaParam.Builder(invalidTableSchema).withTimeout(20).build();
        Response createTableResponse = client.createTable(invalidTableSchemaParam);
        assertFalse(createTableResponse.ok());
        assertEquals(Response.Status.ILLEGAL_TABLE_NAME, createTableResponse.getStatus());
    }

    @org.junit.jupiter.api.Test
    void hasTable() {
        HasTableResponse hasTableResponse = client.hasTable(tableParam);
        assertTrue(hasTableResponse.getResponse().ok());
    }

    @org.junit.jupiter.api.Test
    void dropTable() {
        String nonExistingTableName = generator.generate(10);
        TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
        Response dropTableResponse = client.dropTable(tableParam);
        assertFalse(dropTableResponse.ok());
        assertEquals(Response.Status.TABLE_NOT_EXISTS, dropTableResponse.getStatus());
    }

    @org.junit.jupiter.api.Test
    void createIndex() {
        Index index = new Index.Builder()
                                .withIndexType(IndexType.IVF_SQ8)
                                .withNList(16384)
                                .build();
        IndexParam indexParam = new IndexParam.Builder(randomTableName)
                                                .withIndex(index)
                                                .build();
        Response createIndexResponse = client.createIndex(indexParam);
        assertTrue(createIndexResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void insert() {
        Random random = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; ++j) {
                vector.add(random.nextFloat());
            }
            vectors.add(vector);
        }
        InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
        InsertResponse insertResponse = client.insert(insertParam);
        assertTrue(insertResponse.getResponse().ok());
        assertEquals(size, insertResponse.getVectorIds().size());
    }

    @org.junit.jupiter.api.Test
    void search() throws InterruptedException {
        Random random = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        List<List<Float>> vectorsToSearch = new ArrayList<>();
        int searchSize = 5;
        for (int i = 0; i < size; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; ++j) {
                vector.add(random.nextFloat());
            }
            vectors.add(vector);
            if (i < searchSize) {
                vectorsToSearch.add(vector);
            }
        }
        InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
        InsertResponse insertResponse = client.insert(insertParam);
        assertTrue(insertResponse.getResponse().ok());
        assertEquals(size, insertResponse.getVectorIds().size());

        TimeUnit.SECONDS.sleep(1);

        List<DateRange> queryRanges = new ArrayList<>();
        Calendar rightNow = Calendar.getInstance();
        Date startDate = new Calendar.Builder()
                                     .setDate(rightNow.get(Calendar.YEAR), rightNow.get(Calendar.MONTH) , rightNow.get(Calendar.DAY_OF_MONTH) - 1)
                                     .build()
                                     .getTime();
        Date endDate = new Calendar.Builder()
                                   .setDate(rightNow.get(Calendar.YEAR), rightNow.get(Calendar.MONTH), rightNow.get(Calendar.DAY_OF_MONTH) + 1)
                                   .build()
                                   .getTime();
        queryRanges.add(new DateRange(startDate, endDate));
        System.out.println(queryRanges);
        SearchParam searchParam = new SearchParam
                                        .Builder(randomTableName, vectorsToSearch)
                                        .withTopK(1)
                                        .withNProbe(20)
                                        .withDateRanges(queryRanges)
                                        .build();
        SearchResponse searchResponse = client.search(searchParam);
        assertTrue(searchResponse.getResponse().ok());
        System.out.println(searchResponse);
        assertEquals(searchSize, searchResponse.getQueryResultsList().size());
    }

//    @org.junit.jupiter.api.Test
//    void searchInFiles() {
//    }

    @org.junit.jupiter.api.Test
    void describeTable() {
        DescribeTableResponse describeTableResponse = client.describeTable(tableParam);
        assertTrue(describeTableResponse.getResponse().ok());
        assertTrue(describeTableResponse.getTableSchema().isPresent());

        String nonExistingTableName = generator.generate(10);
        TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
        describeTableResponse = client.describeTable(tableParam);
        assertFalse(describeTableResponse.getResponse().ok());
        assertFalse(describeTableResponse.getTableSchema().isPresent());
    }

    @org.junit.jupiter.api.Test
    void showTables() {
        ShowTablesResponse showTablesResponse = client.showTables();
        assertTrue(showTablesResponse.getResponse().ok());
    }

    @org.junit.jupiter.api.Test
    void getTableRowCount() throws InterruptedException {
        insert();
        TimeUnit.SECONDS.sleep(1);

        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableParam);
        assertTrue(getTableRowCountResponse.getResponse().ok());
        assertEquals(size, getTableRowCountResponse.getTableRowCount());
    }

    @org.junit.jupiter.api.Test
    void deleteByRange() {
        Calendar rightNow = Calendar.getInstance();
        Date startDate = new Calendar.Builder()
                .setDate(rightNow.get(Calendar.YEAR), rightNow.get(Calendar.MONTH) , rightNow.get(Calendar.DAY_OF_MONTH) - 1)
                .build()
                .getTime();
        Date endDate = new Calendar.Builder()
                .setDate(rightNow.get(Calendar.YEAR), rightNow.get(Calendar.MONTH), rightNow.get(Calendar.DAY_OF_MONTH) + 1)
                .build()
                .getTime();
        DeleteByRangeParam deleteByRangeParam = new DeleteByRangeParam.Builder(
                new DateRange(startDate, endDate), randomTableName).build();
        Response deleteByRangeResponse = client.deleteByRange(deleteByRangeParam);
        assertTrue(deleteByRangeResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void preloadTable() {
        Response preloadTableResponse = client.preloadTable(tableParam);
        assertTrue(preloadTableResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void describeIndex() {
        DescribeIndexResponse describeIndexResponse = client.describeIndex(tableParam);
        assertTrue(describeIndexResponse.getResponse().ok());
        assertTrue(describeIndexResponse.getIndexParam().isPresent());
    }

    @org.junit.jupiter.api.Test
    void dropIndex() {
        Response dropIndexResponse = client.dropIndex(tableParam);
        assertTrue(dropIndexResponse.ok());
    }
}