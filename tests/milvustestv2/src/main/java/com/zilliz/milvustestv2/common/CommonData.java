package com.zilliz.milvustestv2.common;

import io.milvus.v2.common.DataType;

/**
 * @Author yongpeng.li
 * @Date 2024/1/31 18:03
 */
public class CommonData {
    public static long numberEntities = 5000;
    public static long batchSize = 1000;
    public static int dim = 128;
    public static int maxCapacity = 100;
    public static String defaultFloatVectorCollection = "FloatVectorCollection";
    public static String defaultBinaryVectorCollection = "BinaryVectorCollection";
    public static String defaultFloat16VectorCollection = "Float16VectorCollection";
    public static String defaultBFloat16VectorCollection = "BFloat16VectorCollection";
    public static String defaultSparseFloatVectorCollection = "SparseFloatVectorCollection";
    public static String partitionNameA = "partitionNameA";
    public static String partitionNameB = "partitionNameB";
    public static String partitionNameC = "partitionNameC";
    public static String defaultPartitionName = "_default";
    public static String fieldInt64 = "fieldInt64";
    public static String fieldInt32 = "fieldInt32";
    public static String fieldInt16 = "fieldInt16";
    public static String fieldInt8 = "fieldInt8";
    public static String fieldDouble = "fieldDouble";
    public static String fieldArray = "fieldArray";
    public static String fieldBool = "fieldBool";
    public static String fieldVarchar = "fieldVarchar";
    public static String fieldFloat = "fieldFloat";
    public static String fieldJson = "fieldJson";
    public static String dynamicField="dynamicField";
    public static String fieldDynamic = "fieldDynamic";
    public static String fieldDynamicNotExist = "fieldDynamicNotExist";
    public static String fieldFloatVector = "fieldFloatVector";
    public static String fieldBinaryVector = "fieldBinaryVector";
    public static String fieldFloat16Vector = "fieldFloat16Vector";
    public static String fieldBF16Vector = "fieldBF16Vector";
    public static String fieldSparseVector = "fieldSparseVector";
    public static int addMaxLength = 99;

    // Struct related fields
    public static String defaultStructCollection = "StructCollection";
    public static String fieldStruct = "fieldStruct";
    public static String structFieldInt32 = "structInt32";
    public static String structFieldVarchar = "structVarchar";
    public static String structFieldFloatVector1 = "structFloatVector1";
    public static String structFieldFloatVector2 = "structFloatVector2";
    public static int structVectorDim = 128;
    public static int structMaxCapacity = 100;


    public static String partitionName = "partitionName";
    // 快速创建时候的默认向量filed
    public static String simpleVector = "vector";
    public static String simplePk = "id";
    public static String alias = "ColAlias";

    public static int topK = 10;
    public static int nq = 1;

    public static String rootUser = "root";

    public static String userName = "user1";
    public static String password = "password1";

    public static String roleName = "role02";

    public static String databaseName = "database00";
    public static String databaseName2 = "database02";
    public static String databaseName1 = "database01";

    public static short defaultValueShort = 1;
    public static int defaultValueInt = 1;
    public static float defaultValueFloat = 1.0F;
    public static double defaultValueDouble = 1.0;
    public static boolean defaultValueBool = true;
    public static String defaultValueString = "1.0";

    public static int groupSize = 3;
    public static String resourceGroup = "resourceGroup01";
    public static String defaultResourceGroup = "__default_resource_group";
    public static String resourceGroup2 = "resourceGroup02";


    public static String providerFieldNameByDatatype(DataType dataType) {
        switch (dataType) {
            case VarChar:
                return fieldVarchar;
            case Int64:
                return fieldInt64;
            case Int8:
                return fieldInt8;
            case Int16:
                return fieldInt16;
            case Int32:
                return fieldInt32;
            case Float:
                return fieldFloat;
            case Double:
                return fieldDouble;
            case FloatVector:
                return fieldFloatVector;
            case BinaryVector:
                return fieldBinaryVector;
            case Float16Vector:
                return fieldFloat16Vector;
            case BFloat16Vector:
                return fieldBF16Vector;
            case SparseFloatVector:
                return fieldSparseVector;
            case Bool:
                return fieldBool;
            case Array:
                return fieldArray;
            case JSON:
                return fieldJson;
            default:
                return fieldDynamic;
        }
    }


}
