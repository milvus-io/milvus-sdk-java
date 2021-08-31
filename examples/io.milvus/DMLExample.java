
import com.google.protobuf.ByteString;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class DMLExample {

    public static MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost("10.2.58.130")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    @Test
    public void insert() {
        // save two vectors : id=101,value=[101,103,107,108] ; id=102,value=[111,112,113,114]
        // primary field's name is xp , vector field name is vector
        // collection name is test

        String collectionName = "test";

        List<String> fieldNames = Arrays.asList("xp", "vector");
        List<DataType> dataTypes = Arrays.asList(DataType.Int64, DataType.FloatVector);

        // primaryKey ids
        List<Long> ids = new ArrayList<Long>() {{
            add(101L);
            add(102L);
        }};

        // vectors' list
        List<List<Float>> vectors = new ArrayList<>();

        //vector1
        List<Float> vector1 = new ArrayList<Float>() {{
            add(101f);
            add(103f);
            add(107f);
            add(108f);
        }};

        // vector2
        List<Float> vector2 = new ArrayList<Float>() {{
            add(100f);
            add(107f);
            add(107f);
            add(108f);
        }};

        vectors.add(vector1);
        vectors.add(vector2);

        // fieldValues; for ids, <?> is Long; for vectors <?> is List<Float>
        List<List<?>> result = new ArrayList<>();
        result.add(ids);
        result.add(vectors);

        InsertParam insertParam = InsertParam.Builder.nweBuilder(collectionName)
                .setFieldNum(2)
                .setFieldNames(fieldNames)
                .setPartitionName("pT")
                .setDataTypes(dataTypes)
                .setFieldValues(result)
                .build();

        R<MutationResult> insert = milvusClient.insert(insertParam);
        System.out.println(insert.getData());


        R<FlushResponse> flush = milvusClient.flush(collectionName);

        System.out.println(flush.getData());
    }

    @Test
    public void delete() {
        DeleteParam build = DeleteParam.Builder.nweBuilder().setCollectionName("test")
                .setPartitionName("pT")
                .build();
        R<MutationResult> delete = milvusClient.delete(build);
        System.out.println(delete.getData());
    }

    @Test
    public void search() {
        SearchRequest.Builder builder = SearchRequest.newBuilder();

        List<Float> vectors = Arrays.asList(0.5271567582442388f, 0.5931217080837329f, 0.8344559910613274f, 0.34260289743736394f);

        HashMap<String, String> params = new HashMap<String, String>(){{
            put("params","{\"nprobe\":10}");
        }};
        SearchParam searchParam = SearchParam.Builder.newBuilder()
                .setCollectionName("hello_milvus")
                .setMetricType(MetricType.L2)
                .setOutFields(Arrays.asList("count", "random_value"))
                .setTopK(5)
                .setVectors(Collections.singletonList(vectors))
                .setVectorFieldName("float_vector")
                .setDslType(DslType.BoolExprV1)
                .setDsl("count > 100")
                .setParams(params)
                .build();


        R<SearchResults> search = milvusClient.search(searchParam);
        System.out.println(search);
    }
}
