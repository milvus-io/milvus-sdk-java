package io.milvus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.GetIndexBuildProgressResponse;
import io.milvus.grpc.GetIndexStateResponse;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.grpc.ShowType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.collection.ShowCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexBuildProgressParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.HasPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
import io.milvus.param.partition.ShowPartitionParam;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Milvus ddl api example
 *
 * @author changzechuan
 */
public class DDLExample {

    public static MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    @Test
    public void createCollection(){
        FieldType[] fieldTypes = new FieldType[2];
        FieldType fieldType1 = FieldType.Builder.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true).build();

        Map<String,String> typeParamsMap = new HashMap<>();
        typeParamsMap.put("dim","512");
        FieldType fieldType2 = FieldType.Builder.newBuilder()
                .withName("vector1")
                .withDescription("match Vector")
                .withDataType(DataType.FloatVector)
                .withTypeParams(typeParamsMap).build();
        fieldTypes[0] = fieldType1;
        fieldTypes[1] = fieldType2;

        CreateCollectionParam createCollectionReq = CreateCollectionParam.Builder.newBuilder()
                .withCollectionName("collection1")
                .withDescription("first collection")
                .withShardsNum(2)
                .withFieldTypes(fieldTypes).build();
        R<RpcStatus> createCollection = milvusClient.createCollection(createCollectionReq);

        System.out.println(createCollection);
    }

    @Test
    public void dropCollection(){
        R<RpcStatus> dropCollection = milvusClient.dropCollection(DropCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(dropCollection);
    }

    @Test
    public void hasCollection(){
        R<Boolean> hasCollection = milvusClient.hasCollection(HasCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());

        System.out.println(hasCollection);
    }

    @Test
    public void loadCollection(){
        R<RpcStatus> loadCollection = milvusClient.loadCollection(LoadCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(loadCollection);
    }

    @Test
    public void releaseCollection(){
        R<RpcStatus> releaseCollection = milvusClient.releaseCollection(ReleaseCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(releaseCollection);
    }

    @Test
    public void describeCollection(){
        R<DescribeCollectionResponse> describeCollection = milvusClient.describeCollection(DescribeCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(describeCollection);
    }

    @Test
    public void getCollectionStatistics(){
        R<GetCollectionStatisticsResponse> getCollectionStatistics = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(getCollectionStatistics);
    }

    @Test
    public void showCollections(){
        String[] collectionNames = new String[]{"collection1"};
        R<ShowCollectionsResponse> showCollections = milvusClient.showCollections(ShowCollectionParam.Builder
                .newBuilder()
                .withCollectionNames(collectionNames)
                .withShowType(ShowType.All)
                .build());
        System.out.println(showCollections);
    }

    @Test
    public void createPartition(){
        R<RpcStatus> createPartition = milvusClient.createPartition(CreatePartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("par1")
                .build());

        System.out.println(createPartition);
    }

    @Test
    public void dropPartition(){
        R<RpcStatus> dropPartition = milvusClient.dropPartition(DropPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("par1")
                .build());

        System.out.println(dropPartition);
    }

    @Test
    public void hasPartition(){
        R<Boolean> hasPartition = milvusClient.hasPartition(HasPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("par1")
                .build());

        System.out.println(hasPartition);
    }

    @Test
    public void loadPartitions(){
        String[] partitionNames = new String[]{"par1"};
        R<RpcStatus> loadPartition = milvusClient.loadPartitions(LoadPartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitionNames)
                .build());

        System.out.println(loadPartition);
    }

    @Test
    public void releasePartitions(){
        String[] releaseNames = new String[]{"par1"};
        R<RpcStatus> releasePartition = milvusClient.releasePartitions(ReleasePartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(releaseNames)
                .build());

        System.out.println(releasePartition);
    }

    @Test
    public void getPartitionStatistics(){
        R<GetPartitionStatisticsResponse> getPartitionStatistics = milvusClient.getPartitionStatistics(GetPartitionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("par1")
                .build());

        System.out.println(getPartitionStatistics);
    }

    @Test
    public void showPartitions(){
        R<ShowPartitionsResponse> showPartitionsResponse = milvusClient.showPartitions(ShowPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        System.out.println(showPartitionsResponse);
    }

    @Test
    public void createIndex(){
        Map<String,String> extraParam = new HashMap<>();
        extraParam.put("index_type","IVF_FLAT");
        extraParam.put("metric_type", "IP");
        extraParam.put("params","{\"nlist\":10}");
        R<RpcStatus> createIndex = milvusClient.createIndex(CreateIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("vector1")
                .withExtraParam(extraParam)
                .build());
        System.out.println(createIndex);
    }

    @Test
    public void dropIndex(){
        R<RpcStatus> dropIndex = milvusClient.dropIndex(DropIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("vector1")
                .build());
        System.out.println(dropIndex);
    }

    @Test
    public void describeIndex(){
        R<DescribeIndexResponse> describeIndex = milvusClient.describeIndex(DescribeIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("vector1")
                .build());
        System.out.println(describeIndex);
    }

    @Test
    public void getIndexState(){
        R<GetIndexStateResponse> getIndexState = milvusClient.getIndexState(GetIndexStateParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("vector1")
                .build());
        System.out.println(getIndexState);
    }

    @Test
    public void getIndexBuildProgress(){
        R<GetIndexBuildProgressResponse> getIndexBuildProgress = milvusClient.getIndexBuildProgress(GetIndexBuildProgressParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("vector1")
                .withIndexName("_default_idx")
                .build());
        System.out.println(getIndexBuildProgress);
    }
}