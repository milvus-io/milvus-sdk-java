package src.io.milvus;

import com.google.protobuf.ByteString;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.dml.*;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class DMLExample {

    public static MilvusServiceClient milvusClient;

    //vector1
    List<Float> vector1 = Arrays.asList(101f, 103f, 107f, 108f);
    // vector2
    List<Float> vector2 = Arrays.asList(111f, 112f, 113f, 114f);

    static {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost("localhost")
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
        String partitionName = "pT";

        List<String> fieldNames = Arrays.asList("xp", "vector");
        List<DataType> dataTypes = Arrays.asList(DataType.Int64, DataType.FloatVector);

        // primaryKey ids
        List<Long> ids = Arrays.asList(101L, 102L);

        // vectors' list
        List<List<Float>> vectors = new ArrayList<>();

        vectors.add(vector1);
        vectors.add(vector2);

        // fieldValues; for ids, <?> is Long; for vectors <?> is List<Float>
        List<List<?>> result = new ArrayList<>();
        result.add(ids);
        result.add(vectors);

        InsertParam insertParam = InsertParam.Builder
                .nweBuilder(collectionName)
                .setFieldNum(fieldNames.size())
                .setFieldNames(fieldNames)
                .setPartitionName(partitionName)
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
        String collectionName = "test";
        String partitionName = "pT";
        DeleteParam build = DeleteParam.Builder.nweBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName)
                .build();
        R<MutationResult> delete = milvusClient.delete(build);
        System.out.println(delete.getData());
    }

    @Test
    public void search() {
        String collectionName = "test";
        String fieldName = "vector";
        List<String> outFields = Collections.singletonList("xp");

        HashMap<String, String> params = new HashMap<String, String>() {{
            put("params", "{\"nprobe\":10}");
        }};
        SearchParam searchParam = SearchParam.Builder.newBuilder()
                .setCollectionName(collectionName)
                .setMetricType(MetricType.L2)
                .setOutFields(outFields)
                .setTopK(5)
                .setVectors(Collections.singletonList(vector1))
                .setVectorFieldName(fieldName)
                .setDslType(DslType.BoolExprV1)
                .setDsl("xp > 100")
                .setParams(params)
                .build();


        R<SearchResults> search = milvusClient.search(searchParam);
        System.out.println(search);
    }

    @Test
    public void calDistance() {
        CalcDistanceParam calcDistanceParam = new CalcDistanceParam(vector1, vector2, MetricType.L2);
        R<CalcDistanceResults> calcDistanceResultsR = milvusClient.calcDistance(calcDistanceParam);
        System.out.println(calcDistanceResultsR);
    }

    @Test
    public void query() {
        String collectionName = "test";
        QueryParam test = QueryParam.Builder.newBuilder("xp in [ 101, 102]")
                .setCollectionName(collectionName)
                .build();
        R<QueryResults> query = milvusClient.query(test);
        System.out.println(query);
    }
}
