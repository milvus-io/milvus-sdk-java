/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.unit.v1.response;

import com.google.protobuf.ByteString;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.FieldType;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class ResponseWrapperTest {
    @Test
    void r() {
        String msg = "error";
        R<RpcStatus> r = R.failed(ErrorCode.UnexpectedError, msg);
        Exception e = r.getException();
        assertEquals(0, msg.compareTo(e.getMessage()));
        System.out.println(r);

        r = R.success();
        assertEquals(R.Status.Success.getCode(), r.getStatus());
        System.out.println(r);
    }

    /// /////////////////////////////////////////////////////////////////////////////////
    // Response wrapper test
    private void testScalarField(ScalarField field, DataType type, long rowCount) {
        FieldData fieldData = FieldData.newBuilder()
                .setFieldName("scalar")
                .setFieldId(1L)
                .setType(type)
                .setScalars(field)
                .build();

        FieldDataWrapper wrapper = new FieldDataWrapper(fieldData);
        assertEquals(rowCount, wrapper.getRowCount());

        List<?> data = wrapper.getFieldData();
        assertEquals(rowCount, data.size());

        assertThrows(IllegalResponseException.class, wrapper::getDim);
    }

    @Test
    void testDescCollResponseWrapper() {
        String collName = "test";
        String collDesc = "test col";
        long collId = 100;
        int shardNum = 10;
        long utcTs = 9999;
        List<String> aliases = Collections.singletonList("a1");

        String fieldName = "f1";
        String fieldDesc = "f1 field";
        final boolean autoId = false;
        final boolean primaryKey = true;
        DataType dt = DataType.Double;
        int dim = 256;
        KeyValuePair kv = KeyValuePair.newBuilder()
                .setKey(Constant.VECTOR_DIM).setValue(String.valueOf(dim)).build();
        FieldSchema field = FieldSchema.newBuilder()
                .setName(fieldName)
                .setDescription(fieldDesc)
                .setAutoID(autoId)
                .setIsPrimaryKey(primaryKey)
                .setDataType(dt)
                .addTypeParams(kv)
                .build();

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setName(collName)
                .setDescription(collDesc)
                .addFields(field)
                .build();

        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionID(collId)
                .addAllAliases(aliases)
                .setShardsNum(shardNum)
                .setCreatedUtcTimestamp(utcTs)
                .setSchema(schema)
                .build();

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(response);
        assertEquals(collName, wrapper.getCollectionName());
        assertEquals(collDesc, wrapper.getCollectionDescription());
        assertEquals(collId, wrapper.getCollectionID());
        assertEquals(shardNum, wrapper.getShardNumber());
        assertEquals(aliases.size(), wrapper.getAliases().size());
        assertEquals(utcTs, wrapper.getCreatedUtcTimestamp());
        assertEquals(1, wrapper.getFields().size());

        assertNull(wrapper.getFieldByName(""));

        FieldType ft = wrapper.getFieldByName(fieldName);
        assertEquals(fieldName, ft.getName());
        assertEquals(fieldDesc, ft.getDescription());
        assertEquals(dt, ft.getDataType());
        assertEquals(autoId, ft.isAutoID());
        assertEquals(primaryKey, ft.isPrimaryKey());
        assertEquals(dim, ft.getDimension());

        assertFalse(wrapper.toString().isEmpty());
    }

    @Test
    void testDescIndexResponseWrapper() {
        final long indexId = 888;
        String indexName = "idx";
        String fieldName = "f1";
        IndexType indexType = IndexType.IVF_FLAT;
        MetricType metricType = MetricType.IP;
        String extraParam = "{nlist:10}";
        KeyValuePair kvIndexType = KeyValuePair.newBuilder()
                .setKey(Constant.INDEX_TYPE).setValue(indexType.name()).build();
        KeyValuePair kvMetricType = KeyValuePair.newBuilder()
                .setKey(Constant.METRIC_TYPE).setValue(metricType.name()).build();
        KeyValuePair kvExtraParam = KeyValuePair.newBuilder()
                .setKey(Constant.PARAMS).setValue(extraParam).build();
        IndexDescription desc = IndexDescription.newBuilder()
                .setIndexID(indexId)
                .setIndexName(indexName)
                .setFieldName(fieldName)
                .addParams(kvIndexType)
                .addParams(kvMetricType)
                .addParams(kvExtraParam)
                .build();
        DescribeIndexResponse response = DescribeIndexResponse.newBuilder()
                .addIndexDescriptions(desc)
                .build();

        DescIndexResponseWrapper wrapper = new DescIndexResponseWrapper(response);
        assertEquals(1, wrapper.getIndexDescriptions().size());
        assertNull(wrapper.getIndexDescByFieldName(""));

        DescIndexResponseWrapper.IndexDesc indexDesc = wrapper.getIndexDescByFieldName(fieldName);
        assertEquals(indexId, indexDesc.getId());
        assertEquals(indexName, indexDesc.getIndexName());
        assertEquals(fieldName, indexDesc.getFieldName());
        assertEquals(indexType, indexDesc.getIndexType());
        assertEquals(metricType, indexDesc.getMetricType());
        String params = indexDesc.getExtraParam();
        assertEquals(0, extraParam.compareTo(params.replace("\"", "")));

        assertFalse(wrapper.toString().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testFieldDataWrapper() {
        // for float vector
        long dim = 3;
        List<Float> floatVectors = Arrays.asList(1F, 2F, 3F, 4F, 5F, 6F);
        FieldData fieldData = FieldData.newBuilder()
                .setFieldName("vec")
                .setFieldId(1L)
                .setType(DataType.FloatVector)
                .setVectors(VectorField.newBuilder()
                        .setDim(dim)
                        .setFloatVector(FloatArray.newBuilder()
                                .addAllData(floatVectors)
                                .build())
                        .build())
                .build();

        FieldDataWrapper wrapper = new FieldDataWrapper(fieldData);
        assertEquals(dim, wrapper.getDim());
        assertEquals(floatVectors.size() / dim, wrapper.getRowCount());

        List<?> floatData = wrapper.getFieldData();
        assertEquals(floatVectors.size() / dim, floatData.size());
        for (Object obj : floatData) {
            List<Float> vec = (List<Float>) obj;
            assertEquals(dim, vec.size());
        }

        // for binary vector
        dim = 16;
        int bytesPerVec = (int) (dim / 8);
        int count = 2;
        byte[] binary = new byte[bytesPerVec * count];
        for (int i = 0; i < binary.length; ++i) {
            binary[i] = (byte) i;
        }
        fieldData = FieldData.newBuilder()
                .setFieldName("vec")
                .setFieldId(1L)
                .setType(DataType.BinaryVector)
                .setVectors(VectorField.newBuilder()
                        .setDim(dim)
                        .setBinaryVector(ByteString.copyFrom(binary))
                        .build())
                .build();

        wrapper = new FieldDataWrapper(fieldData);
        assertEquals(dim, wrapper.getDim());
        assertEquals(count, wrapper.getRowCount());

        List<?> binaryData = wrapper.getFieldData();
        assertEquals(count, binaryData.size());
        for (int i = 0; i < binaryData.size(); i++) {
            ByteBuffer vec = (ByteBuffer) binaryData.get(i);
            assertEquals(bytesPerVec, vec.limit());

            for (int j = 0; j < bytesPerVec; j++) {
                assertEquals(binary[i * bytesPerVec + j], vec.get(j));
            }
        }

        // for scalar field
        LongArray.Builder int64Builder = LongArray.newBuilder();
        for (long i = 0; i < dim; ++i) {
            int64Builder.addData(i);
        }
        testScalarField(ScalarField.newBuilder().setLongData(int64Builder).build(),
                DataType.Int64, dim);

        IntArray.Builder intBuilder = IntArray.newBuilder();
        for (int i = 0; i < dim; ++i) {
            intBuilder.addData(i);
        }
        testScalarField(ScalarField.newBuilder().setIntData(intBuilder).build(),
                DataType.Int32, dim);
        testScalarField(ScalarField.newBuilder().setIntData(intBuilder).build(),
                DataType.Int16, dim);
        testScalarField(ScalarField.newBuilder().setIntData(intBuilder).build(),
                DataType.Int8, dim);

        BoolArray.Builder boolBuilder = BoolArray.newBuilder();
        for (long i = 0; i < dim; ++i) {
            boolBuilder.addData(i % 2 == 0);
        }
        testScalarField(ScalarField.newBuilder().setBoolData(boolBuilder).build(),
                DataType.Bool, dim);

        FloatArray.Builder floatBuilder = FloatArray.newBuilder();
        for (long i = 0; i < dim; ++i) {
            floatBuilder.addData((float) i);
        }
        testScalarField(ScalarField.newBuilder().setFloatData(floatBuilder).build(),
                DataType.Float, dim);

        DoubleArray.Builder doubleBuilder = DoubleArray.newBuilder();
        for (long i = 0; i < dim; ++i) {
            doubleBuilder.addData((double) i);
        }
        testScalarField(ScalarField.newBuilder().setDoubleData(doubleBuilder).build(),
                DataType.Double, dim);

        StringArray.Builder strBuilder = StringArray.newBuilder();
        for (long i = 0; i < dim; ++i) {
            strBuilder.addData(String.valueOf(i));
        }
        testScalarField(ScalarField.newBuilder().setStringData(strBuilder).build(),
                DataType.VarChar, dim);
    }

    @Test
    void testGetCollStatResponseWrapper() {
        GetCollectionStatisticsResponse response = GetCollectionStatisticsResponse.newBuilder()
                .addStats(KeyValuePair.newBuilder().setKey(Constant.ROW_COUNT).setValue("invalid").build())
                .build();
        GetCollStatResponseWrapper invalidWrapper = new GetCollStatResponseWrapper(response);
        assertThrows(NumberFormatException.class, invalidWrapper::getRowCount);

        response = GetCollectionStatisticsResponse.newBuilder()
                .addStats(KeyValuePair.newBuilder().setKey(Constant.ROW_COUNT).setValue("10").build())
                .build();
        GetCollStatResponseWrapper wrapper = new GetCollStatResponseWrapper(response);
        assertEquals(10, wrapper.getRowCount());

        response = GetCollectionStatisticsResponse.newBuilder().build();
        wrapper = new GetCollStatResponseWrapper(response);
        assertEquals(0, wrapper.getRowCount());
    }

    @Test
    void testGetPartStatResponseWrapper() {
        final long rowCount = 500;
        KeyValuePair kvStat = KeyValuePair.newBuilder()
                .setKey(Constant.ROW_COUNT).setValue(String.valueOf(rowCount)).build();
        GetPartitionStatisticsResponse response = GetPartitionStatisticsResponse.newBuilder()
                .addStats(kvStat).build();

        GetPartStatResponseWrapper wrapper = new GetPartStatResponseWrapper(response);
        assertEquals(rowCount, wrapper.getRowCount());

        response = GetPartitionStatisticsResponse.newBuilder().build();

        wrapper = new GetPartStatResponseWrapper(response);
        assertEquals(0, wrapper.getRowCount());
    }

    @Test
    void testMutationResultWrapper() {
        List<Long> nID = Arrays.asList(1L, 2L, 3L);
        MutationResult results = MutationResult.newBuilder()
                .setInsertCnt(nID.size())
                .setDeleteCnt(nID.size())
                .setIDs(IDs.newBuilder()
                        .setIntId(LongArray.newBuilder()
                                .addAllData(nID)
                                .build()))
                .setTimestamp(1000)
                .build();
        MutationResultWrapper longWrapper = new MutationResultWrapper(results);
        assertEquals(1000, longWrapper.getOperationTs());
        assertEquals(nID.size(), longWrapper.getInsertCount());
        assertEquals(nID.size(), longWrapper.getDeleteCount());
        assertThrows(ParamException.class, longWrapper::getStringIDs);

        List<Long> longIDs = longWrapper.getLongIDs();
        assertEquals(nID.size(), longIDs.size());
        for (int i = 0; i < longIDs.size(); ++i) {
            assertEquals(nID.get(i), longIDs.get(i));
        }

        List<String> sID = Arrays.asList("1", "2", "3");
        results = MutationResult.newBuilder()
                .setInsertCnt(sID.size())
                .setIDs(IDs.newBuilder()
                        .setStrId(StringArray.newBuilder()
                                .addAllData(sID)
                                .build()))
                .build();
        MutationResultWrapper strWrapper = new MutationResultWrapper(results);
        assertEquals(sID.size(), strWrapper.getInsertCount());
        assertThrows(ParamException.class, strWrapper::getLongIDs);

        List<String> strIDs = strWrapper.getStringIDs();
        assertEquals(sID.size(), strIDs.size());
        for (int i = 0; i < strIDs.size(); ++i) {
            assertEquals(sID.get(i), strIDs.get(i));
        }
    }

    @Test
    void testQueryResultsWrapper() {
        String fieldName = "test";
        QueryResults results = QueryResults.newBuilder()
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName(fieldName)
                        .build())
                .build();

        QueryResultsWrapper wrapper = new QueryResultsWrapper(results);
        assertThrows(ParamException.class, () -> wrapper.getFieldWrapper("invalid"));
        assertNotNull(wrapper.getFieldWrapper(fieldName));
    }

    @Test
    void testQueryResultsWrapperWithNullStructField() {
        FieldData ratingField = FieldData.newBuilder()
                .setFieldName("rating")
                .setType(DataType.Array)
                .setScalars(ScalarField.newBuilder()
                        .setArrayData(ArrayArray.newBuilder()
                                .addData(ScalarField.newBuilder()
                                        .setIntData(IntArray.newBuilder().addData(5).build())
                                        .build())
                                .addData(ScalarField.newBuilder().build())
                                .build())
                        .build())
                .addValidData(true)
                .addValidData(false)
                .build();
        FieldData tagField = FieldData.newBuilder()
                .setFieldName("tag")
                .setType(DataType.Array)
                .setScalars(ScalarField.newBuilder()
                        .setArrayData(ArrayArray.newBuilder()
                                .addData(ScalarField.newBuilder()
                                        .setStringData(StringArray.newBuilder().addData("favorite").build())
                                        .build())
                                .addData(ScalarField.newBuilder().build())
                                .build())
                        .build())
                .addValidData(true)
                .addValidData(false)
                .build();
        FieldData metadataField = FieldData.newBuilder()
                .setFieldName("metadata")
                .setType(DataType.ArrayOfStruct)
                .setStructArrays(StructArrayField.newBuilder()
                        .addFields(ratingField)
                        .addFields(tagField)
                        .build())
                .build();
        FieldData idField = FieldData.newBuilder()
                .setFieldName("id")
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder()
                        .setLongData(LongArray.newBuilder().addData(1L).addData(2L).build())
                        .build())
                .build();
        QueryResults results = QueryResults.newBuilder()
                .addFieldsData(idField)
                .addFieldsData(metadataField)
                .addOutputFields("id")
                .addOutputFields("metadata")
                .build();

        QueryResultsWrapper wrapper = new QueryResultsWrapper(results);
        List<QueryResultsWrapper.RowRecord> records = wrapper.getRowRecords();

        assertEquals(2, records.size());
        assertNotNull(records.get(0).get("metadata"));
        assertNull(records.get(1).get("metadata"));
    }

    @Test
    void testSearchResultsWrapper() {
        long topK = 5;
        long numQueries = 2;
        List<Long> longIDs = new ArrayList<>();
        List<String> strIDs = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        List<Double> outputField = new ArrayList<>();
        for (long i = 0; i < topK * numQueries; ++i) {
            longIDs.add(i);
            strIDs.add(String.valueOf(i));
            scores.add((float) i);
            outputField.add((double) i);
        }

        // for long id
        DoubleArray.Builder doubleArrayBuilder = DoubleArray.newBuilder();
        outputField.forEach(doubleArrayBuilder::addData);

        String fieldName = "test";
        SearchResultData results = SearchResultData.newBuilder()
                .setTopK(topK)
                .addTopks(topK)
                .addTopks(topK) // numQueries=2, the topks list must have 2 elements
                .setNumQueries(numQueries)
                .setIds(IDs.newBuilder()
                        .setIntId(LongArray.newBuilder()
                                .addAllData(longIDs)
                                .build()))
                .addAllScores(scores)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName(fieldName)
                        .setType(DataType.Double)
                        .setScalars(ScalarField.newBuilder()
                                .setDoubleData(doubleArrayBuilder.build())
                                .build()))
                .build();

        SearchResultsWrapper intWrapper = new SearchResultsWrapper(results);
        assertThrows(ParamException.class, () -> intWrapper.getFieldData(fieldName, -1));
        assertThrows(ParamException.class, () -> intWrapper.getFieldData("invalid", 0));
        assertEquals(topK, intWrapper.getFieldData(fieldName, (int) numQueries - 1).size());

        List<SearchResultsWrapper.IDScore> idScores = intWrapper.getIDScore(1);
        assertFalse(idScores.toString().isEmpty());
        assertEquals(topK, idScores.size());
        assertThrows(ParamException.class, () -> intWrapper.getIDScore((int) numQueries));

        // for string id
        results = SearchResultData.newBuilder()
                .setTopK(topK)
                .addTopks(topK)
                .addTopks(topK) // numQueries=2, the topks list must have 2 elements
                .setNumQueries(numQueries)
                .setIds(IDs.newBuilder()
                        .setStrId(StringArray.newBuilder()
                                .addAllData(strIDs)
                                .build()))
                .addAllScores(scores)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName(fieldName)
                        .build())
                .build();

        SearchResultsWrapper strWrapper = new SearchResultsWrapper(results);
        idScores = strWrapper.getIDScore(0);
        assertFalse(idScores.toString().isEmpty());
        assertEquals(topK, idScores.size());

        idScores.forEach((score) -> assertFalse(score.toString().isEmpty()));
    }

    @Test
    void testShowCollResponseWrapper() {
        List<String> names = Arrays.asList("coll_1", "coll_2");
        List<Long> ids = Arrays.asList(1L, 2L);
        List<Long> ts = Arrays.asList(888L, 999L);
        List<Long> inMemory = Arrays.asList(100L, 50L);
        ShowCollectionsResponse response = ShowCollectionsResponse.newBuilder()
                .addAllCollectionNames(names)
                .addAllCollectionIds(ids)
                .addAllCreatedUtcTimestamps(ts)
                .addAllInMemoryPercentages(inMemory)
                .build();

        ShowCollResponseWrapper wrapper = new ShowCollResponseWrapper(response);
        assertEquals(names.size(), wrapper.getCollectionsInfo().size());
        assertFalse(wrapper.toString().isEmpty());

        for (int i = 0; i < 2; ++i) {
            ShowCollResponseWrapper.CollectionInfo info = wrapper.getCollectionInfoByName(names.get(i));
            assertEquals(0, names.get(i).compareTo(info.getName()));
            assertEquals(ids.get(i), info.getId());
            assertEquals(ts.get(i), info.getUtcTimestamp());
            assertEquals(inMemory.get(i), info.getInMemoryPercentage());

            assertFalse(info.toString().isEmpty());
        }
    }

    @Test
    void testShowPartResponseWrapper() {
        List<String> names = Arrays.asList("part_1", "part_2");
        List<Long> ids = Arrays.asList(1L, 2L);
        List<Long> ts = Arrays.asList(888L, 999L);
        List<Long> inMemory = Arrays.asList(100L, 50L);
        ShowPartitionsResponse response = ShowPartitionsResponse.newBuilder()
                .addAllPartitionNames(names)
                .addAllPartitionIDs(ids)
                .addAllCreatedUtcTimestamps(ts)
                .addAllInMemoryPercentages(inMemory)
                .build();

        ShowPartResponseWrapper wrapper = new ShowPartResponseWrapper(response);
        assertEquals(names.size(), wrapper.getPartitionsInfo().size());
        assertFalse(wrapper.toString().isEmpty());

        for (int i = 0; i < 2; ++i) {
            ShowPartResponseWrapper.PartitionInfo info = wrapper.getPartitionInfoByName(names.get(i));
            assertEquals(0, names.get(i).compareTo(info.getName()));
            assertEquals(ids.get(i), info.getId());
            assertEquals(ts.get(i), info.getUtcTimestamp());
            assertEquals(inMemory.get(i), info.getInMemoryPercentage());

            assertFalse(info.toString().isEmpty());
        }
    }

    @Test
    void testGetBulkInsertStateWrapper() {
        long count = 1000;
        ImportState state = ImportState.ImportStarted;
        String reason = "unexpected error";
        String files = "1.json";
        String collection = "c1";
        String partition = "p1";
        String progress = "50";
        GetImportStateResponse resp = GetImportStateResponse.newBuilder()
                .setState(state)
                .setRowCount(count)
                .addIdList(0)
                .addIdList(99)
                .addInfos(KeyValuePair.newBuilder()
                        .setKey(Constant.FAILED_REASON)
                        .setValue(reason)
                        .build())
                .addInfos(KeyValuePair.newBuilder()
                        .setKey(Constant.IMPORT_FILES)
                        .setValue(files)
                        .build())
                .addInfos(KeyValuePair.newBuilder()
                        .setKey(Constant.IMPORT_COLLECTION)
                        .setValue(collection)
                        .build())
                .addInfos(KeyValuePair.newBuilder()
                        .setKey(Constant.IMPORT_PARTITION)
                        .setValue(partition)
                        .build())
                .addInfos(KeyValuePair.newBuilder()
                        .setKey(Constant.IMPORT_PROGRESS)
                        .setValue(progress)
                        .build())
                .build();

        GetBulkInsertStateWrapper wrapper = new GetBulkInsertStateWrapper(resp);
        assertEquals(count, wrapper.getImportedCount());
        assertEquals(100, wrapper.getAutoGeneratedIDs().size());
        assertEquals(0, wrapper.getAutoGeneratedIDs().get(0));
        assertEquals(99, wrapper.getAutoGeneratedIDs().get(99));
        assertEquals(reason, wrapper.getFailedReason());
        assertEquals(files, wrapper.getFiles());
        assertEquals(collection, wrapper.getCollectionName());
        assertEquals(partition, wrapper.getPartitionName());
        assertEquals(progress, String.valueOf(wrapper.getProgress()));

        assertFalse(wrapper.toString().isEmpty());
    }
}
