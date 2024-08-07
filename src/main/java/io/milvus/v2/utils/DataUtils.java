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

package io.milvus.v2.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.FieldType;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.NonNull;

import java.util.*;

public class DataUtils {

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;

        public InsertRequest convertGrpcInsertRequest(@NonNull InsertReq requestParam,
                                                      DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getData().size());
            upsertBuilder = null;
            fillFieldsData(requestParam, wrapper);
            return insertBuilder.build();
        }

        public UpsertRequest convertGrpcUpsertRequest(@NonNull UpsertReq requestParam,
                                                      DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // currently, not allow to upsert for collection whose primary key is auto-generated
            FieldType pk = wrapper.getPrimaryField();
            if (pk.isAutoID()) {
                throw new ParamException(String.format("Upsert don't support autoID==True, collection: %s",
                        requestParam.getCollectionName()));
            }

            // generate upsert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getData().size());
            insertBuilder = null;
            fillFieldsData(requestParam, wrapper);
            return upsertBuilder.build();
        }

        private void addFieldsData(io.milvus.grpc.FieldData value) {
            if (insertBuilder != null) {
                insertBuilder.addFieldsData(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.addFieldsData(value);
            }
        }

        private void setPartitionName(String value) {
            if (insertBuilder != null) {
                insertBuilder.setPartitionName(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.setPartitionName(value);
            }
        }

        private void fillFieldsData(UpsertReq requestParam, DescCollResponseWrapper wrapper) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            boolean isPartitionKeyEnabled = false;
            for (FieldType fieldType : wrapper.getFields()) {
                if (fieldType.isPartitionKey()) {
                    isPartitionKeyEnabled = true;
                    break;
                }
            }
            if (isPartitionKeyEnabled) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new ParamException(msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(wrapper, rowFields);
        }

        private void fillFieldsData(InsertReq requestParam, DescCollResponseWrapper wrapper) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            boolean isPartitionKeyEnabled = false;
            for (FieldType fieldType : wrapper.getFields()) {
                if (fieldType.isPartitionKey()) {
                    isPartitionKeyEnabled = true;
                    break;
                }
            }
            if (isPartitionKeyEnabled) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new ParamException(msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(wrapper, rowFields);
        }

        private void checkAndSetRowData(DescCollResponseWrapper wrapper, List<JsonObject> rows) {
            List<FieldType> fieldTypes = wrapper.getFields();

            Map<String, ParamUtils.InsertDataInfo> nameInsertInfo = new HashMap<>();
            ParamUtils.InsertDataInfo insertDynamicDataInfo = ParamUtils.InsertDataInfo.builder().fieldType(
                            FieldType.newBuilder()
                                    .withName(Constant.DYNAMIC_FIELD_NAME)
                                    .withDataType(DataType.JSON)
                                    .withIsDynamic(true)
                                    .build())
                    .data(new LinkedList<>()).build();
            for (JsonObject row : rows) {
                for (FieldType fieldType : fieldTypes) {
                    String fieldName = fieldType.getName();
                    ParamUtils.InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, ParamUtils.InsertDataInfo.builder()
                            .fieldType(fieldType).data(new LinkedList<>()).build());

                    // check normalField
                    JsonElement rowFieldData = row.get(fieldName);
                    if (rowFieldData != null) {
                        if (fieldType.isAutoID()) {
                            String msg = String.format("The primary key: %s is auto generated, no need to input.", fieldName);
                            throw new ParamException(msg);
                        }
                        Object fieldValue = ParamUtils.checkFieldValue(fieldType, rowFieldData);
                        insertDataInfo.getData().add(fieldValue);
                        nameInsertInfo.put(fieldName, insertDataInfo);
                    } else {
                        // check if autoId
                        if (!fieldType.isAutoID()) {
                            String msg = String.format("The field: %s is not provided.", fieldType.getName());
                            throw new ParamException(msg);
                        }
                    }
                }

                // deal with dynamicField
                if (wrapper.getEnableDynamicField()) {
                    JsonObject dynamicField = new JsonObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!nameInsertInfo.containsKey(rowFieldName)) {
                            dynamicField.add(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.getData().add(dynamicField);
                }
            }

            for (String fieldNameKey : nameInsertInfo.keySet()) {
                ParamUtils.InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
                this.addFieldsData(ParamUtils.genFieldData(insertDataInfo.getFieldType(), insertDataInfo.getData()));
            }
            if (wrapper.getEnableDynamicField()) {
                this.addFieldsData(ParamUtils.genFieldData(insertDynamicDataInfo.getFieldType(), insertDynamicDataInfo.getData(), Boolean.TRUE));
            }
        }
    }
}
