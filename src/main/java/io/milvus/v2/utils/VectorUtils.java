package io.milvus.v2.utils;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class VectorUtils {

    public QueryRequest ConvertToGrpcQueryRequest(QueryReq request){
//        long guaranteeTimestamp = getGuaranteeTimestamp(ConsistencyLevelEnum.valueOf(request.getConsistencyLevel().name()),
//                request.getGuaranteeTimestamp(), request.getGracefulTime());
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames())
                .addAllOutputFields(request.getOutputFields())
                .setExpr(request.getFilter());

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        // set offset and limit value.
        // directly pass the two values, the server will verify them.
        long offset = request.getOffset();
        if (offset > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.OFFSET)
                    .setValue(String.valueOf(offset))
                    .build());
        }

        long limit = request.getLimit();
        if (limit > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.LIMIT)
                    .setValue(String.valueOf(limit))
                    .build());
        }

        // ignore growing
//        builder.addQueryParams(KeyValuePair.newBuilder()
//                .setKey(Constant.IGNORE_GROWING)
//                .setValue(String.valueOf(request.isIgnoreGrowing()))
//                .build());

        return builder.build();

    }

    private static long getGuaranteeTimestamp(ConsistencyLevelEnum consistencyLevel,
                                              long guaranteeTimestamp, Long gracefulTime){
        if(consistencyLevel == null){
            return 1L;
        }
        switch (consistencyLevel){
            case STRONG:
                guaranteeTimestamp = 0L;
                break;
            case BOUNDED:
                guaranteeTimestamp = (new Date()).getTime() - gracefulTime;
                break;
            case EVENTUALLY:
                guaranteeTimestamp = 1L;
                break;
        }
        return guaranteeTimestamp;
    }

    public SearchRequest ConvertToGrpcSearchRequest(@NotNull String metricType, SearchReq request) {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setDbName("")
                .setCollectionName(request.getCollectionName());
        if (!request.getPartitionNames().isEmpty()) {
            request.getPartitionNames().forEach(builder::addPartitionNames);
        }

        builder.addSearchParams(
                KeyValuePair.newBuilder()
                        .setKey(Constant.METRIC_TYPE)
                        .setValue(metricType)
                        .build());


        // prepare target vectors
        // TODO: check target vector dimension(use DescribeCollection get schema to compare)
        PlaceholderType plType = PlaceholderType.None;
        List<?> vectors = request.getData();
        List<ByteString> byteStrings = new ArrayList<>();
        for (Object vector : vectors) {
            if (vector instanceof List) {
                plType = PlaceholderType.FloatVector;
                List<Float> list = (List<Float>) vector;
                ByteBuffer buf = ByteBuffer.allocate(Float.BYTES * list.size());
                buf.order(ByteOrder.LITTLE_ENDIAN);
                list.forEach(buf::putFloat);

                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else if (vector instanceof ByteBuffer) {
                plType = PlaceholderType.BinaryVector;
                ByteBuffer buf = (ByteBuffer) vector;
                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else {
                String msg = "Search target vector type is illegal(Only allow List<Float> or ByteBuffer)";
                throw new ParamException(msg);
            }
        }

        PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                .setTag(Constant.VECTOR_TAG)
                .setType(plType);
        byteStrings.forEach(pldBuilder::addValues);

        PlaceholderValue plv = pldBuilder.build();
        PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                .addPlaceholders(plv)
                .build();

        ByteString byteStr = placeholderGroup.toByteString();
        builder.setPlaceholderGroup(byteStr);
        //builder.setNq(request.getNQ());

        // search parameters
        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.VECTOR_FIELD)
                                .setValue(request.getVectorFieldName())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(request.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.ROUND_DECIMAL)
                                .setValue(String.valueOf(request.getRoundDecimal()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.IGNORE_GROWING)
                                .setValue(String.valueOf(request.isIgnoreGrowing()))
                                .build());

        builder.addSearchParams(
                KeyValuePair.newBuilder()
                        .setKey(Constant.OFFSET)
                        .setValue(String.valueOf(request.getOffset()))
                        .build());

        if (null != request.getSearchParams()) {
            try {
                Gson gson = new Gson();
                String searchParams = gson.toJson(request.getSearchParams());
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.PARAMS)
                                .setValue(searchParams)
                                .build());
            } catch (IllegalArgumentException e) {
                throw new ParamException(e.getMessage() + e.getCause().getMessage());
            }
        }

        if (!request.getOutputFields().isEmpty()) {
            request.getOutputFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            builder.setDsl(request.getFilter());
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(ConsistencyLevelEnum.valueOf(request.getConsistencyLevel().name()),
                request.getGuaranteeTimestamp(), request.getGracefulTime());
        //builder.setTravelTimestamp(request.getTravelTimestamp());
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    public String getExprById(String primaryFieldName, List<?> ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(primaryFieldName).append(" in [");
        for (Object id : ids) {
            if (id instanceof String) {
                sb.append("\"").append(id.toString()).append("\",");
            } else {
                sb.append(id.toString()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }
}
