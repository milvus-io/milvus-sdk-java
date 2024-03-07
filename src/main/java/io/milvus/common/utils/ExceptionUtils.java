package io.milvus.common.utils;

import io.milvus.exception.UnExpectedException;
import io.milvus.param.R;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionUtils {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionUtils.class);

    public static void throwUnExpectedException(String msg) {
        logger.error(msg);
        throw new UnExpectedException(msg);
    }

    public static void handleResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }
}
