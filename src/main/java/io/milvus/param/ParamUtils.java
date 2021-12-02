package io.milvus.param;

import io.milvus.exception.ParamException;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility functions for param classes
 */
public class ParamUtils {
    /**
     * Checks if a string is empty or null.
     * Throws {@link ParamException} if the string is empty of null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullEmptyString(String target, String name) throws ParamException {
        if (target == null || StringUtils.isBlank(target)) {
            throw new ParamException(name + " cannot be null or empty");
        }
    }
}
