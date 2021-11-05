package io.milvus.param;

import io.milvus.exception.ParamException;
import org.apache.commons.lang3.StringUtils;

public class ParamUtils {
    public static void CheckNullEmptyString(String target, String name) throws ParamException {
        if (target == null || StringUtils.isBlank(target)) {
            throw new ParamException(name + " cannot be null or empty");
        }
    }
}
