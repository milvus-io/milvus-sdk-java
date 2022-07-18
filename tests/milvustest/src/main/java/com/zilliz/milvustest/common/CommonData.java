package com.zilliz.milvustest.common;

public class CommonData {
  public static String defaultCollection = "CollectionAutoTest";
  public static String defaultBinaryCollection = "BinaryCollectionAutoTest";
  public static String defaultStringPKCollection = "CollectionStringPKAutoTest";
  public static String defaultStringPKBinaryCollection = "BinaryCollectionStringPKAutoTest";
  public static String defaultPartition = "PartitionAutoTest";
  public static String defaultBinaryPartition = "BinaryPartitionAutoTest";
  public static String defaultStringPKPartition = "PartitionStringPKAutoTest";
  public static String defaultStringPKBinaryPartition = "BinaryPartitionStringPKAutoTest";
  public static String defaultAlias = "AliasAutoTest";
  public static String defaultStringPKAlias = "StringPKAliasAutoTest";
  public static String defaultBinaryAlias = "BinaryAliasAutoTest";
  public static String defaultStringPKBinaryAlias = "StringPKBinaryAliasAutoTest";
  public static String defaultVectorField = "VectorFieldAutoTest";
  public static String defaultBinaryVectorField = "BinaryVectorFieldAutoTest";
  public static String defaultIndex = "FloatVectorIndex";

  public static String defaultBinaryIndex = "BinaryVectorIndex";
  public static String defaultExtraParam =
      "{\"nlist\":1024,\"M\":16,\"efConstruction\":64, \"PQM\":16,\"nbits\":8}";
  public static String defaultUserName = "UserNameAT";
  public static String defaultPassword = "Password123AT";
  public static String defaultBulkLoadPath = "./src/test/java/resources/temp/";
  public static String defaultRowJson = "rowJson";
  public static String defaultRowStrJson = "rowStrJson";
  public static String defaultColJson = "colJson";
  public static String defaultColStrJson = "colStrJson";
}
