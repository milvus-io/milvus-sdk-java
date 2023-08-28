package io.milvus.common.utils;

import io.milvus.exception.ParamException;
import io.milvus.param.collection.FieldType;
import io.milvus.response.DescCollResponseWrapper;
import org.apache.logging.log4j.util.Strings;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class VectorUtils {
    public static String convertPksExpr(List<?> primaryIds, DescCollResponseWrapper wrapper) {
        Optional<FieldType> optional = wrapper.getFields().stream().filter(FieldType::isPrimaryKey).findFirst();
        String expr;
        if (optional.isPresent()) {
            FieldType primaryField = optional.get();
            switch (primaryField.getDataType()) {
                case Int64:
                    List<String> primaryStringIds = primaryIds.stream().map(String::valueOf).collect(Collectors.toList());
                    expr = convertPksExpr(primaryStringIds, primaryField.getName());
                    break;
                case VarChar:
                    List<String> primaryVarcharIds = primaryIds.stream().map(primaryId -> String.format("\"%s\"", primaryId)).collect(Collectors.toList());
                    expr = convertPksExpr(primaryVarcharIds, primaryField.getName());
                    break;
                default:
                    throw new ParamException("The primary key is not of type int64 or varchar, and the current operation is not supported.");
            }
        } else {
            throw new ParamException("No primary key found.");
        }
        return expr;
    }

    public static String convertPksExpr(List<?> primaryIds, String primaryFieldName) {
        return primaryFieldName + " in [" + Strings.join(primaryIds, ',') + "]";
    }
}
