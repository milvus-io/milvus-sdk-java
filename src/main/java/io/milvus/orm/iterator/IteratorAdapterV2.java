package io.milvus.orm.iterator;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.grpc.PlaceholderType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.SearchIteratorParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.data.BaseVector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class IteratorAdapterV2 {
    public static QueryIteratorParam convertV2Req(QueryIteratorReq queryIteratorReq) {
        return QueryIteratorParam.newBuilder()
                .withDatabaseName(queryIteratorReq.getDatabaseName())
                .withCollectionName(queryIteratorReq.getCollectionName())
                .withPartitionNames(queryIteratorReq.getPartitionNames())
                .withExpr(queryIteratorReq.getExpr())
                .withOutFields(queryIteratorReq.getOutputFields())
                .withConsistencyLevel(ConsistencyLevelEnum.valueOf(queryIteratorReq.getConsistencyLevel().name()))
                .withOffset(queryIteratorReq.getOffset())
                .withLimit(queryIteratorReq.getLimit())
                .withIgnoreGrowing(queryIteratorReq.isIgnoreGrowing())
                .withBatchSize(queryIteratorReq.getBatchSize())
                .build();
    }
    public static SearchIteratorParam convertV2Req(SearchIteratorReq searchIteratorReq) {
        MetricType metricType = MetricType.None;
        if (searchIteratorReq.getMetricType() != IndexParam.MetricType.INVALID) {
            metricType = MetricType.valueOf(searchIteratorReq.getMetricType().name());
        }

        SearchIteratorParam.Builder builder = SearchIteratorParam.newBuilder()
                .withDatabaseName(searchIteratorReq.getDatabaseName())
                .withCollectionName(searchIteratorReq.getCollectionName())
                .withPartitionNames(searchIteratorReq.getPartitionNames())
                .withVectorFieldName(searchIteratorReq.getVectorFieldName())
                .withMetricType(metricType)
                .withTopK(searchIteratorReq.getTopK())
                .withExpr(searchIteratorReq.getExpr())
                .withOutFields(searchIteratorReq.getOutputFields())
                .withRoundDecimal(searchIteratorReq.getRoundDecimal())
                .withParams(searchIteratorReq.getParams())
                .withGroupByFieldName(searchIteratorReq.getGroupByFieldName())
                .withIgnoreGrowing(searchIteratorReq.isIgnoreGrowing())
                .withBatchSize(searchIteratorReq.getBatchSize());

        if (searchIteratorReq.getConsistencyLevel() != null) {
            builder.withConsistencyLevel(ConsistencyLevelEnum.valueOf(searchIteratorReq.getConsistencyLevel().name()));
        }

        List<BaseVector> vectors = searchIteratorReq.getVectors();
        PlaceholderType plType = vectors.get(0).getPlaceholderType();
        for (BaseVector vector : vectors) {
            if (vector.getPlaceholderType() != plType) {
                throw new ParamException("Different types of target vectors in a search request is not allowed.");
            }
        }

        switch (plType) {
            case FloatVector: {
                List<List<Float>> data = new ArrayList<>();
                vectors.forEach(vector->data.add((List<Float>)vector.getData()));
                builder.withFloatVectors(data);
                break;
            }
            case BinaryVector: {
                List<ByteBuffer> data = new ArrayList<>();
                vectors.forEach(vector->data.add((ByteBuffer)vector.getData()));
                builder.withBinaryVectors(data);
                break;
            }
            case Float16Vector: {
                List<ByteBuffer> data = new ArrayList<>();
                vectors.forEach(vector -> data.add((ByteBuffer)vector.getData()));
                builder.withFloat16Vectors(data);
                break;
            }
            case BFloat16Vector: {
                List<ByteBuffer> data = new ArrayList<>();
                vectors.forEach(vector -> data.add((ByteBuffer)vector.getData()));
                builder.withBFloat16Vectors(data);
                break;
            }
            case SparseFloatVector: {
                List<SortedMap<Long, Float>> data = new ArrayList<>();
                vectors.forEach(vector -> data.add((SortedMap<Long, Float>)vector.getData()));
                builder.withSparseFloatVectors(data);
                break;
            }
            default:
                throw new ParamException("Unsupported vector type.");
        }

        return builder.build();
    }

    public static FieldType convertV2Field(CreateCollectionReq.FieldSchema schema) {
        FieldType.Builder builder = FieldType.newBuilder()
                .withName(schema.getName())
                .withDataType(DataType.valueOf(schema.getDataType().name()))
                .withPrimaryKey(schema.getIsPrimaryKey())
                .withAutoID(schema.getAutoID())
                .withPartitionKey(schema.getIsPartitionKey());

        if (schema.getDimension() != null) {
            builder.withDimension(schema.getDimension());
        }
        if (schema.getMaxLength() != null) {
            builder.withMaxLength(schema.getMaxLength());
        }
        if (schema.getMaxCapacity() != null) {
            builder.withMaxCapacity(schema.getMaxLength());
        }
        if (schema.getElementType() != null) {
            builder.withElementType(DataType.valueOf(schema.getElementType().name()));
        }
        return builder.build();
    }
}
