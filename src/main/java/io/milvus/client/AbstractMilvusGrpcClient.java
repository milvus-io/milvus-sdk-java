package io.milvus.client;

import io.grpc.StatusRuntimeException;
import io.milvus.grpc.*;
import io.milvus.param.Control.GetMetricsRequestParam;
import io.milvus.param.Control.GetPersistentSegmentInfoParam;
import io.milvus.param.Control.GetQuerySegmentInfoParam;
import io.milvus.param.R;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    protected abstract boolean maybeAvailable();
    @Override
    public R<GetMetricsResponse>getMetrics(GetMetricsRequestParam getMetricsRequestParam){
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }
        if(getMetricsRequestParam==null || getMetricsRequestParam.getRequest()==null || !isJson(getMetricsRequestParam.getRequest())){
            return R.failed(R.Status.ParamError);
        }

        GetMetricsRequest getMetricsRequest = GetMetricsRequest.newBuilder()
                .setRequest(getMetricsRequestParam.getRequest())
                .build();
        GetMetricsResponse response;
        try {
            response = blockingStub().getMetrics(getMetricsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get metrics successfully!\n{}", getMetricsRequestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getMetrics RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }
    @Override
    public R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(GetQuerySegmentInfoParam getQuerySegmentInfoParam){
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }
        //GetQuerySegmentInfoParam is  null or collection name is  null,return parameter error.
        if(getQuerySegmentInfoParam==null||getQuerySegmentInfoParam.getCollectionName()==null){
            return R.failed(R.Status.ParamError);
        }
        GetQuerySegmentInfoRequest getQuerySegmentInfoRequest = GetQuerySegmentInfoRequest.newBuilder()
                .setCollectionName(getQuerySegmentInfoParam.getCollectionName())
                .build();
        GetQuerySegmentInfoResponse response;
        try {
            response = blockingStub().getQuerySegmentInfo(getQuerySegmentInfoRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get query segment info successfully!\n{}", getQuerySegmentInfoParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getQuerySegmentInfo RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }
    @Override
    public R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(GetPersistentSegmentInfoParam getPersistentSegmentInfoParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }
        if(getPersistentSegmentInfoParam==null||getPersistentSegmentInfoParam.getCollectionName()==null){
            return R.failed(R.Status.ParamError);
        }
        GetPersistentSegmentInfoRequest getPersistentSegmentInfoRequest =
                GetPersistentSegmentInfoRequest.newBuilder()
                .setCollectionName(getPersistentSegmentInfoParam.getCollectionName()).build();

        GetPersistentSegmentInfoResponse response;

        try {
            response = blockingStub().getPersistentSegmentInfo(getPersistentSegmentInfoRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get persistent segment info successfully!\n{}", getPersistentSegmentInfoParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getPersistentSegmentInfo RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasCollection(String collectionName) {
        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .build();

        BoolResponse response;
        try {
            response = blockingStub().hasCollection(hasCollectionRequest);
        } catch (StatusRuntimeException e) {
            logger.error("[milvus] hasCollection:{} request error: {}", collectionName, e.getMessage());
            return R.failed(e);
        }
        Boolean aBoolean = Optional.ofNullable(response)
                .map(BoolResponse::getValue)
                .orElse(false);
        return R.success(aBoolean);
    }
    //Simply check whether it is json
    public  boolean isJson(String str) {
        boolean result = false;
        if (StringUtils.isNotBlank(str)) {
            str = str.trim();
            if ( str.startsWith("{") && str.endsWith( "}")) {
                result = true;
            }else if(str.startsWith("[")&&str.endsWith("]")){
                result = true;
            }
        }
        return result;
    }

    private boolean checkServerConnection() {
        if (!maybeAvailable()) {
            logWarning("You are not connected to Milvus server");
            return true;
        }
        return false;
    }
    private void logInfo(String msg, Object... params) {
        logger.info(msg, params);
    }

    private void logWarning(String msg, Object... params) {
        logger.warn(msg, params);
    }

    private void logError(String msg, Object... params) {
        logger.error(msg, params);
    }
}


