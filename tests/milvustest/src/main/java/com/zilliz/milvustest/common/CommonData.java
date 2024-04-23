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
  public static String defaultVectorField = "book_intro";
  public static String defaultPartitionField="book_name";
  public static String defaultBinaryVectorField = "BinaryVectorFieldAutoTest";
  public static String defaultFloat16VectorField = "Float16VectorField";
  public static String defaultBF16VectorField = "BF16VectorField";
  public static String defaultSparseVectorField = "SparseFloatVectorField";
  public static String defaultIndex = "FloatVectorIndex";

  public static String defaultBinaryIndex = "BinaryVectorIndex";
  public static String defaultExtraParam =
          "{\"nlist\":128}";
  public static String defaultUserName = "UserNameAT";
  public static String defaultPassword = "Password123AT";
  public static String defaultBulkLoadPath = "./src/test/java/resources/temp/";
  public static String defaultBulkWritePath = "./src/test/java/resources/temp/bulkWrite/";
  public static String defaultRowJson = "rowJson";
  public static String defaultRowStrJson = "rowStrJson";
  public static String defaultColJson = "colJson";
  public static String defaultColStrJson = "colStrJson";
  public static String defaultRoleName = "roleTest";
  public static String defaultRoleName2 = "roleTest2";
  public static String databaseName1="db1";
  public static String databaseName2="db2";

  public static int dim=128;
}

