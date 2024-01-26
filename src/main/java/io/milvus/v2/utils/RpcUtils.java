package io.milvus.v2.utils;

import io.milvus.exception.ServerException;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.Status;
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

        if (status.getCode() != 0 || !status.getErrorCode().equals(ErrorCode.Success)) {
            logger.error("{} failed, error code: {}, reason: {}", requestInfo,
                    status.getCode() > 0 ? status.getCode() : status.getErrorCode().getNumber(),
                    status.getReason());

            // 2.3.4 sdk to interact with 2.2.x server, the getCode() is zero, here we reset its value to getErrorCode()
            int code = status.getCode();
            if (code == 0) {
                code = status.getErrorCode().getNumber();
            }
            throw new ServerException(status.getReason(), code, status.getErrorCode());
        }

        logger.debug("{} successfully!", requestInfo);
    }
}
