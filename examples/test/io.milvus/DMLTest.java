package io.milvus;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.DeleteParam;
import io.milvus.param.InsertParam;
import io.milvus.param.R;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DMLTest {

    public static MilvusClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost("10.2.58.130")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    @Test
    public void insert(){
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
    public void delete(){
        DeleteParam build = DeleteParam.Builder.nweBuilder().setCollectionName("test")
                .setPartitionName("pT")
                .build();
        R<MutationResult> delete = milvusClient.delete(build);
        System.out.println(delete.getData());
    }
}
