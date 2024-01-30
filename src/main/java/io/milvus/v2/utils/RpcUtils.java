package io.milvus.v2.utils;

import io.milvus.grpc.Status;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcUtils {

    protected static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);

    public void handleResponse(String requestInfo, Status status) {
        // the server made a change for error code:
        // for 2.2.x, error code is status.getErrorCode()
        // for 2.3.x, error code is status.getCode(), and the status.getErrorCode()
        // is also assigned according to status.getCode()
        //
        // For error cases:
        // if we use 2.3.4 sdk to interact with 2.3.x server, getCode() is non-zero, getErrorCode() is non-zero
        // if we use 2.3.4 sdk to interact with 2.2.x server, getCode() is zero, getErrorCode() is non-zero
        // if we use <=2.3.3 sdk to interact with 2.2.x/2.3.x server, getCode() is not available, getErrorCode() is non-zero

        if (status.getCode() != 0 || !status.getErrorCode().equals(io.milvus.grpc.ErrorCode.Success)) {

            // 2.3.4 sdk to interact with 2.2.x server, the getCode() is zero, here we reset its value to getErrorCode()
            int code = status.getCode();
            if (code == 0) {
                code = status.getErrorCode().getNumber();
            }
            logger.error("{} failed, error code: {}, reason: {}", requestInfo, ErrorCode.SERVER_ERROR.getCode(),
                    status.getReason());
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, status.getReason());
        }

        logger.debug("{} successfully!", requestInfo);
    }
}
