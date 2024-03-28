import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.nio.ByteBuffer;
import java.util.*;

public class BinaryVectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_sparse";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";

    private static final Integer VECTOR_DIM = 512;

    private static List<ByteBuffer> generateVectors(int count) {
        Random ran = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        int byteCount = VECTOR_DIM / 8;
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = ByteBuffer.allocate(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
            }
            vectors.add(vector);
        }
        return vectors;

    }

    private static void handleResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }

    public static void main(String[] args) {
        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());

        // drop the collection if you don't need the collection anymore
        R<Boolean> hasR = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        handleResponseStatus(hasR);
        if (hasR.getData()) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .build());
        }

        // Define fields
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(DataType.BinaryVector)
                        .withDimension(VECTOR_DIM)
                        .build()
        );

        // Create the collection
        R<RpcStatus> ret = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withFieldTypes(fieldsSchema)
                .build());
        handleResponseStatus(ret);
        System.out.println("Collection created");

        // Insert entities
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
        }
        List<ByteBuffer> vectors = generateVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(ID_FIELD, ids));
        fieldsInsert.add(new InsertParam.Field(VECTOR_FIELD, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertR = milvusClient.insert(insertParam);
        handleResponseStatus(insertR);

        // Flush the data to storage for testing purpose
        // Note that no need to manually call flush interface in practice
        R<FlushResponse> flushR = milvusClient.flush(FlushParam.newBuilder().
                addCollectionName(COLLECTION_NAME).
                build());
        handleResponseStatus(flushR);
        System.out.println("Entities inserted");

        // Specify an index type on the vector field.
        ret = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withExtraParam("{\"nlist\":64}")
                .build());
        handleResponseStatus(ret);
        System.out.println("Index created");

        // Call loadCollection() to enable automatically loading data into memory for searching
        ret = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        handleResponseStatus(ret);
        System.out.println("Collection loaded");

        // Pick some vectors from the inserted vectors to search
        // Ensure the returned top1 item's ID should be equal to target vector's ID
        for (int i = 0; i < 10; i++) {
            Random ran = new Random();
            int k = ran.nextInt(rowCount);
            ByteBuffer targetVector = vectors.get(k);
            R<SearchResults> searchRet = milvusClient.search(SearchParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withMetricType(MetricType.HAMMING)
                    .withTopK(3)
                    .withVectors(Collections.singletonList(targetVector))
                    .withVectorFieldName(VECTOR_FIELD)
                    .addOutField(VECTOR_FIELD)
                    .withParams("{\"nprobe\":16}")
                    .build());
            handleResponseStatus(searchRet);

            // The search() allows multiple target vectors to search in a batch.
            // Here we only input one vector to search, get the result of No.0 vector to check
            SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchRet.getData().getResults());
            List<SearchResultsWrapper.IDScore> scores = resultsWrapper.getIDScore(0);
            System.out.printf("The result of No.%d target vector:\n", i);
            for (SearchResultsWrapper.IDScore score : scores) {
                System.out.printf("ID: %d, Score: %f, Vector: ", score.getLongID(), score.getScore());
                ByteBuffer vector = (ByteBuffer)score.get(VECTOR_FIELD);
                vector.rewind();
                while (vector.hasRemaining()) {
                    System.out.print(Integer.toBinaryString(vector.get()));
                }
                System.out.println();
            }
            if (scores.get(0).getLongID() != k) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d",
                        scores.get(0).getLongID(), k));
            }
        }
        System.out.println("Search result is correct");

        // Retrieve some data
        int n = 99;
        R<QueryResults> queryR = milvusClient.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(String.format("id == %d", n))
                .addOutField(VECTOR_FIELD)
                .build());
        handleResponseStatus(queryR);
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryR.getData());
        FieldDataWrapper field = queryWrapper.getFieldWrapper(VECTOR_FIELD);
        List<?> r = field.getFieldData();
        if (r.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        } else {
            ByteBuffer vector = (ByteBuffer) r.get(0);
            if (vector.compareTo(vectors.get(n)) != 0) {
                throw new RuntimeException("The query result is incorrect");
            }
        }
        System.out.println("Query result is correct");

        // drop the collection if you don't need the collection anymore
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");

        milvusClient.close();
    }
}
