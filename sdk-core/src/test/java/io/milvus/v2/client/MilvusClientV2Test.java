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

package io.milvus.v2.client;

import io.milvus.common.resourcegroup.*;
import io.milvus.param.*;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.alias.ListAliasesParam;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;
import io.milvus.param.bulkinsert.ListBulkInsertTasksParam;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.dml.DeleteIdsParam;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.param.highlevel.dml.QuerySimpleParam;
import io.milvus.param.highlevel.dml.SearchSimpleParam;
import io.milvus.param.highlevel.dml.response.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.param.resourcegroup.*;
import io.milvus.param.role.*;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.BaseTest;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.cdc.request.CrossClusterTopology;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.UpdateReplicateConfigurationResp;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.DescribeReplicasResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;
import io.milvus.v2.service.resourcegroup.request.*;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.ranker.*;
import io.milvus.v2.service.vector.response.*;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.*;
import java.util.*;

public class MilvusClientV2Test extends BaseTest {
    private static final int RANDOM_BOUND = 300;

    private static class CheckConfig {
        public boolean assertGetter;
        public boolean assertSetter;
        public List<String> ignoredMethods;

        public CheckConfig() {
            this.ignoredMethods = new ArrayList<>();
            this.assertGetter = true;
            this.assertSetter = true;
        }

        public CheckConfig(List<String> ignoredMethods) {
            this.ignoredMethods = ignoredMethods;
            this.assertGetter = true;
            this.assertSetter = true;
        }

        public CheckConfig(List<String> ignoredMethods, boolean assertGetter, boolean assertSetter) {
            this.ignoredMethods = ignoredMethods;
            this.assertGetter = assertGetter;
            this.assertSetter = assertSetter;
        }

        public void setIgnoredMethods(List<String> ignoredMethods) {
            this.ignoredMethods = ignoredMethods;
        }

        public void clearIgnoredMethods() {
            this.ignoredMethods = new ArrayList<>();
        }
    }

    private static class BuilderClassDef {
        public Class<?> builderCls;
        public boolean isOldStyle = false;

        public BuilderClassDef(Class<?> builderCls, boolean isOldStyle) {
            this.builderCls = builderCls;
            this.isOldStyle = isOldStyle;
        }
    }

    private static class FieldBinder {
        public final String fieldName;
        public final Field field;
        public Method getter;
        public Method setter;

        public FieldBinder(Field field) {
            this.fieldName = field.getName();
            this.field = field;
            this.getter = null;
            this.setter = null;
        }

        public FieldBinder(Field field, Method getter, Method setter) {
            this.fieldName = field.getName();
            this.field = field;
            this.getter = getter;
            this.setter = setter;
        }
    }

    private static Random random = new Random();
    private static RandomStringGenerator randomStr = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    private static Class<?> getClass(String clsName) {
        try {
            return Class.forName(clsName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void getNonStaticMembers(Class<?> cls, Map<String, Field> members) {
        Field[] fields = cls.getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                members.put(field.getName(), field);
            }
        }
    }

    private static void getNonStaticMethods(Class<?> cls, Map<String, Method> methods) {
        try {
            Method[] mts = cls.getMethods();
            for (Method method : mts) {
                if (!Modifier.isStatic(method.getModifiers())) {
                    methods.put(method.getName(), method);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean isGetter(Method method) {
        return (method.getName().startsWith("get") || method.getName().startsWith("is")) && method.getParameterCount() == 0;
    }

    private static boolean isSetter(Method method) {
        return method.getName().startsWith("set") && method.getParameterCount() == 1;
    }

    private static String capFirstChar(String str) {
        return WordUtils.capitalize(str);
    }

    private static void getGettersAndSetters(Class<?> cls, List<FieldBinder> binders, CheckConfig config) {
        try {
            Map<String, Field> fields = new HashMap<>();
            getNonStaticMembers(cls, fields);

            Method[] methods = cls.getMethods();
            Map<String, Method> allMethods = new HashMap<>();
            for (Method method : methods) {
                allMethods.put(method.getName(), method);
            }

            for (Field field : fields.values()) {
                Class<?> fieldType = field.getType();
                boolean isBooleanType = fieldType.getName().equals("boolean");
                String capName = capFirstChar(field.getName());

                FieldBinder binder = new FieldBinder(field);
                binders.add(binder);

                String getterName = (isBooleanType ? "is" : "get") + capName;
                if (config.assertGetter && !config.ignoredMethods.contains(getterName)) {
                    System.out.println("assert getter: " + getterName);
                    Assertions.assertTrue(allMethods.containsKey(getterName));
                    binder.getter = allMethods.get(getterName);
                    Assertions.assertTrue(isGetter(binder.getter));
                }

                String setterName = "set" + capName;
                if (config.assertSetter && !config.ignoredMethods.contains(setterName)) {
                    System.out.println("assert setter: " + setterName);
                    Assertions.assertTrue(allMethods.containsKey(setterName));
                    binder.setter = allMethods.get(setterName);
                    Assertions.assertTrue(isSetter(binder.setter));
                }
            }
            Assertions.assertEquals(fields.size(), binders.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static BuilderClassDef getBuilderClass(Class<?> cls) {
        try {
            String fullName = cls.getName();
            int max = Math.max(fullName.lastIndexOf("."), fullName.lastIndexOf("$"));
            String pureClsName = fullName.substring(max + 1);
            String newStyleName = cls.getName() + "$" + pureClsName + "Builder";
            String oldStyleName = cls.getName() + "$" + "Builder";
            for (Class<?> subCls : cls.getDeclaredClasses()) {
                if (subCls.getName().equals(newStyleName) || subCls.getName().equals(oldStyleName)) {
                    return new BuilderClassDef(subCls, subCls.getName().equals(oldStyleName));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void randomCallMethod(Field field, Method method, Object callInstance,
                                         Map<String, Object> randomValues, CheckConfig config) {
        Class<?> fieldType = field.getType();
        if (fieldType == Boolean.class || fieldType.getName().equals("boolean")) {
            Boolean obj = random.nextBoolean();
            randomValues.put(field.getName(), obj);
        } else if (fieldType == String.class) {
            String obj = randomStr.generate(10);
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Long.class || fieldType.getName().equals("long")) {
            Long obj = (long) random.nextInt(RANDOM_BOUND);
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Integer.class || fieldType.getName().equals("int")) {
            Integer obj = random.nextInt(RANDOM_BOUND);
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Short.class) {
            Short obj = (short) random.nextInt(RANDOM_BOUND);
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Float.class || fieldType.getName().equals("float")) {
            Float obj = random.nextFloat();
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Double.class || fieldType.getName().equals("double")) {
            Double obj = random.nextDouble();
            randomValues.put(field.getName(), obj);
        } else if (fieldType == Number.class) {
            Number obj = random.nextInt();
            randomValues.put(field.getName(), obj);
        } else if (fieldType == List.class) {
            AnnotatedType tt = field.getAnnotatedType();
            ParameterizedType pType = (ParameterizedType) tt.getType();
            Type[] typeArgs = pType.getActualTypeArguments();
            if (typeArgs[0].equals(String.class)) {
                List<String> obj = Collections.singletonList(randomStr.generate(10));
                randomValues.put(field.getName(), obj);
            } else if (typeArgs[0].equals(Long.class)) {
                List<Long> obj = Collections.singletonList((long) random.nextInt(RANDOM_BOUND));
                randomValues.put(field.getName(), obj);
            } else if (typeArgs[0].equals(Integer.class)) {
                List<Integer> obj = Collections.singletonList(random.nextInt(RANDOM_BOUND));
                randomValues.put(field.getName(), obj);
            } else if (typeArgs[0].equals(Short.class)) {
                List<Short> obj = Collections.singletonList((short) random.nextInt(RANDOM_BOUND));
                randomValues.put(field.getName(), obj);
            } else if (typeArgs[0].equals(Float.class)) {
                List<Float> obj = Collections.singletonList(random.nextFloat());
                randomValues.put(field.getName(), obj);
            } else if (typeArgs[0].equals(Double.class)) {
                List<Double> obj = Collections.singletonList(random.nextDouble());
                randomValues.put(field.getName(), obj);
            } else {
                List<?> obj = new ArrayList<>();
                obj.add(null);
                randomValues.put(field.getName(), obj);
            }
        } else if (fieldType == Map.class) {
            Map<?, ?> obj = new HashMap<>();
            randomValues.put(field.getName(), obj);
        } else if (fieldType.isEnum()) {
            randomValues.put(field.getName(), fieldType.getEnumConstants()[1]);
        } else if (!fieldType.isInterface() && !fieldType.isArray()) {
            if ("java.lang.Object".equals(fieldType.getName())) {
                // some method accepts Object as input such as AddFieldReq.defaultValue
                Long obj = random.nextLong();
                randomValues.put(field.getName(), obj);
            } else {
                // recursive construct member by builder
                Object obj = checkGetAndSet(fieldType, config);
                randomValues.put(field.getName(), obj);
            }
        } else {
            randomValues.put(field.getName(), null);
        }

        try {
            method.invoke(callInstance, randomValues.get(field.getName()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object createBuilderInstance(Class<?> cls) {
        Method builderMethod = null;
        try {
            builderMethod = cls.getMethod("newBuilder");
        } catch (Exception e) {
        }
        if (builderMethod == null) {
            try {
                builderMethod = cls.getMethod("builder");
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }

        try {
            Object builderInstance = builderMethod.invoke(null);
            Assertions.assertNotNull(builderInstance);
            return builderInstance;
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
        return null;
    }

    private static Object buildByBuilder(Class<?> cls, Map<String, Object> randomValues, CheckConfig config) {
        BuilderClassDef builderDef = getBuilderClass(cls);
        if (builderDef == null) {
            return null;
        }

        Map<String, Method> builderMethods = new HashMap<>();
        getNonStaticMethods(builderDef.builderCls, builderMethods);

        Map<String, Field> clsFields = new HashMap<>();
        getNonStaticMembers(cls, clsFields);

        // builder must contain all members of the parent class
        Map<String, Field> builderFields = new HashMap<>();
        getNonStaticMembers(builderDef.builderCls, builderFields);
        Map<String, Field> realBuilderFields = new HashMap<>();
        for (String name : builderFields.keySet()) {
            if (name.endsWith("$set")) {
                continue;
            }
            if (name.endsWith("$value")) {
                realBuilderFields.put(name.substring(0, name.lastIndexOf("$value")), builderFields.get(name));
            } else {
                realBuilderFields.put(name, builderFields.get(name));
            }
        }
        for (String name : clsFields.keySet()) {
            if (!config.ignoredMethods.contains(name)) {
                Assertions.assertTrue(realBuilderFields.containsKey(name));
            }
        }

        Object builderInstance = createBuilderInstance(cls);
        Assertions.assertNotNull(builderInstance);
        for (Field field : clsFields.values()) {
            String capName = capFirstChar(field.getName());
            String withName = builderDef.isOldStyle ? "with" + capName : field.getName();
            if (config.ignoredMethods.contains(withName) || config.ignoredMethods.contains(field.getName())) {
                // some builder classes doesn't have a normal method to set member
                // such as the ResourceGroupNodeFilter.withNodeLabel
                continue;
            }

            System.out.println("call builder method for field: " + field.getName());
            Method buildMethod = null;
            if (builderMethods.containsKey(withName)) {
                buildMethod = builderMethods.get(withName);
            } else if (builderMethods.containsKey(field.getName())) {
                buildMethod = builderMethods.get(field.getName());
            } else {
                System.out.println("failed to get method from builder for field: " + field.getName());
                Assertions.fail();
            }
            randomCallMethod(field, buildMethod, builderInstance, randomValues, config);
        }

        try {
            System.out.println("call build() method of: " + builderDef.builderCls.getName());
            Method buildMethod = builderDef.builderCls.getMethod("build");
            Object clsInstance = buildMethod.invoke(builderInstance);
            System.out.println(clsInstance);
            return clsInstance;
        } catch (Exception e) {
            e.printStackTrace();
            if (!config.ignoredMethods.contains("build")) {
                Assertions.fail();
            }
        }
        return null;
    }

    private static void checkCallResult(Field field, Method method, Object callIntance,
                                        Map<String, Object> randomValues, CheckConfig config) {
        try {
            // some methods are deprecated and be compatible with new method, such as SearchReq.topK
            if (method == null || config.ignoredMethods.contains(method.getName())) {
                return;
            }

            Object temp = method.invoke(callIntance);
            System.out.println("check getter value for method: " + field.getName());
            if (temp instanceof Boolean || temp instanceof String || temp instanceof Number ||
                    temp instanceof List || temp instanceof Map || temp instanceof Enum) {
                Assertions.assertEquals(temp, randomValues.get(field.getName()));
            } else if (!temp.getClass().isInterface() && !temp.getClass().isArray()) {
                Class<?> fieldType = field.getType();
                Assertions.assertEquals(temp.getClass(), fieldType);
            } else {
                Assertions.assertNull(temp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object checkGetAndSet(Class<?> cls, CheckConfig config) {
        Map<String, Field> fields = new HashMap<>();
        getNonStaticMembers(cls, fields);

        List<FieldBinder> binders = new ArrayList<>();
        getGettersAndSetters(cls, binders, config);

        Map<String, Object> randomValues = new HashMap<>();
        Object clsInstance = buildByBuilder(cls, randomValues, config);
        if (clsInstance == null) {
            return null;
        }

        for (FieldBinder binder : binders) {
            checkCallResult(binder.field, binder.getter, clsInstance, randomValues, config);
        }

        return clsInstance;
    }

    private static void VerifyClass(String clsName, CheckConfig config) {
        System.out.println("\n### Verify class " + clsName);
        Class<?> cls = getClass(clsName);
        Assertions.assertNotNull(cls);
        checkGetAndSet(cls, config);
    }

    @Test
    void testV2BuilderClasses() {
        CheckConfig config = new CheckConfig();

        // io.milvus/v2/client
        config.setIgnoredMethods(Arrays.asList("clientRequestId", "getClientRequestId", "setClientRequestId",
                "sslContext", "setSslContext", "getSslContext"));
        VerifyClass(ConnectConfig.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(RetryConfig.class.getName(), config);

        // io.milvus/v2/common
        VerifyClass(IndexParam.class.getName(), config);

        // io.milvus/v2/service/cdc
        VerifyClass(CrossClusterTopology.class.getName(), config);
        VerifyClass(MilvusCluster.class.getName(), config);
        VerifyClass(ReplicateConfiguration.class.getName(), config);
        VerifyClass(UpdateReplicateConfigurationReq.class.getName(), config);

        VerifyClass(UpdateReplicateConfigurationResp.class.getName(), config);

        // io.milvus/v2/service/collection
        VerifyClass(AddCollectionFieldReq.class.getName(), config);
        config.setIgnoredMethods(Collections.singletonList("isEnableDefaultValue"));
        VerifyClass(AddFieldReq.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Collections.singletonList("setProperties"));
        VerifyClass(AlterCollectionFieldReq.class.getName(), config);
        VerifyClass(AlterCollectionPropertiesReq.class.getName(), config);
        VerifyClass(AlterCollectionReq.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(BatchDescribeCollectionReq.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("enableDynamicField", "isEnableDynamicField", "getEnableDynamicField", "setProperties"));
        VerifyClass(CreateCollectionReq.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(CreateCollectionReq.CollectionSchema.class.getName(), config);
        VerifyClass(CreateCollectionReq.Function.class.getName(), config);
        VerifyClass(CreateCollectionReq.StructFieldSchema.class.getName(), config);
        VerifyClass(DescribeCollectionReq.class.getName(), config);
        VerifyClass(DropCollectionFieldPropertiesReq.class.getName(), config);
        VerifyClass(DropCollectionPropertiesReq.class.getName(), config);
        VerifyClass(DropCollectionReq.class.getName(), config);
        VerifyClass(GetCollectionStatsReq.class.getName(), config);
        VerifyClass(GetLoadStateReq.class.getName(), config);
        VerifyClass(HasCollectionReq.class.getName(), config);
        VerifyClass(ListCollectionsReq.class.getName(), config);
        config.setIgnoredMethods(Collections.singletonList("getAsync"));
        VerifyClass(LoadCollectionReq.class.getName(), config);
        VerifyClass(RefreshLoadReq.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(ReleaseCollectionReq.class.getName(), config);
        VerifyClass(RenameCollectionReq.class.getName(), config);

        config.setIgnoredMethods(Collections.singletonList("setProperties"));
        VerifyClass(DescribeCollectionResp.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DescribeReplicasResp.class.getName(), config);
        VerifyClass(GetCollectionStatsResp.class.getName(), config);
        VerifyClass(ListCollectionsResp.class.getName(), config);

        // io.milvus/v2/service/database
        VerifyClass(AlterDatabasePropertiesReq.class.getName(), config);
        VerifyClass(AlterDatabaseReq.class.getName(), config);
        VerifyClass(CreateDatabaseReq.class.getName(), config);
        VerifyClass(DescribeDatabaseReq.class.getName(), config);
        VerifyClass(DropDatabasePropertiesReq.class.getName(), config);
        VerifyClass(DropDatabaseReq.class.getName(), config);

        VerifyClass(DescribeDatabaseResp.class.getName(), config);
        VerifyClass(ListDatabasesResp.class.getName(), config);

        // io.milvus/v2/service/index
        VerifyClass(AlterIndexPropertiesReq.class.getName(), config);
        VerifyClass(AlterIndexReq.class.getName(), config);
        VerifyClass(CreateIndexReq.class.getName(), config);
        VerifyClass(DescribeIndexReq.class.getName(), config);
        VerifyClass(DropIndexPropertiesReq.class.getName(), config);
        VerifyClass(DropIndexReq.class.getName(), config);
        VerifyClass(ListIndexesReq.class.getName(), config);

        VerifyClass(DescribeIndexResp.class.getName(), config);

        // io.milvus/v2/service/partition
        VerifyClass(CreatePartitionReq.class.getName(), config);
        VerifyClass(DropPartitionReq.class.getName(), config);
        VerifyClass(GetPartitionStatsReq.class.getName(), config);
        VerifyClass(HasPartitionReq.class.getName(), config);
        VerifyClass(ListPartitionsReq.class.getName(), config);
        VerifyClass(LoadPartitionsReq.class.getName(), config);
        VerifyClass(ReleasePartitionsReq.class.getName(), config);

        VerifyClass(GetPartitionStatsResp.class.getName(), config);

        // io.milvus/v2/service/rbac
        VerifyClass(AddPrivilegesToGroupReq.class.getName(), config);
        VerifyClass(CreatePrivilegeGroupReq.class.getName(), config);
        VerifyClass(CreateRoleReq.class.getName(), config);
        VerifyClass(CreateUserReq.class.getName(), config);
        VerifyClass(DescribeRoleReq.class.getName(), config);
        VerifyClass(DescribeUserReq.class.getName(), config);
        VerifyClass(DropPrivilegeGroupReq.class.getName(), config);
        VerifyClass(DropRoleReq.class.getName(), config);
        VerifyClass(DropUserReq.class.getName(), config);
        VerifyClass(GrantPrivilegeReq.class.getName(), config);
        VerifyClass(GrantPrivilegeReqV2.class.getName(), config);
        VerifyClass(GrantRoleReq.class.getName(), config);
        VerifyClass(ListPrivilegeGroupsReq.class.getName(), config);
        VerifyClass(RemovePrivilegesFromGroupReq.class.getName(), config);
        VerifyClass(RevokePrivilegeReq.class.getName(), config);
        VerifyClass(RevokePrivilegeReqV2.class.getName(), config);
        VerifyClass(RevokeRoleReq.class.getName(), config);
        VerifyClass(UpdatePasswordReq.class.getName(), config);

        VerifyClass(DescribeRoleResp.class.getName(), config);
        VerifyClass(DescribeRoleResp.GrantInfo.class.getName(), config);
        VerifyClass(DescribeUserResp.class.getName(), config);
        VerifyClass(ListPrivilegeGroupsResp.class.getName(), config);
        VerifyClass(PrivilegeGroup.class.getName(), config);

        // io.milvus/v2/service/resourcegroup
        config.setIgnoredMethods(Arrays.asList("withNodeLabels", "setNodeLabels", "getNodeLabels", "setNodeFilter",
                "setFrom", "setTo", "setRequests", "setLimits", "setNodeNum"));
        VerifyClass(CreateResourceGroupReq.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DescribeResourceGroupReq.class.getName(), config);
        VerifyClass(DropResourceGroupReq.class.getName(), config);
        VerifyClass(ListResourceGroupsReq.class.getName(), config);
        VerifyClass(TransferNodeReq.class.getName(), config);
        VerifyClass(TransferReplicaReq.class.getName(), config);
        VerifyClass(UpdateResourceGroupsReq.class.getName(), config);

        config.setIgnoredMethods(Arrays.asList("withNodeLabels", "setNodeLabels", "getNodeLabels", "setNodeFilter",
                "setFrom", "setTo", "setRequests", "setLimits", "setNodeNum"));
        VerifyClass(DescribeResourceGroupResp.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(ListResourceGroupsResp.class.getName(), config);

        // io.milvus/v2/service/utility
        VerifyClass(AlterAliasReq.class.getName(), config);
        VerifyClass(CompactReq.class.getName(), config);
        VerifyClass(CreateAliasReq.class.getName(), config);
        VerifyClass(DescribeAliasReq.class.getName(), config);
        VerifyClass(DropAliasReq.class.getName(), config);
        VerifyClass(FlushReq.class.getName(), config);
        VerifyClass(GetCompactionStateReq.class.getName(), config);
        VerifyClass(GetPersistentSegmentInfoReq.class.getName(), config);
        VerifyClass(GetQuerySegmentInfoReq.class.getName(), config);
        VerifyClass(ListAliasesReq.class.getName(), config);

        VerifyClass(CheckHealthResp.class.getName(), config);
        VerifyClass(CompactResp.class.getName(), config);
        VerifyClass(DescribeAliasResp.class.getName(), config);
        VerifyClass(FlushResp.class.getName(), config);
        VerifyClass(GetCompactionStateResp.class.getName(), config);
        VerifyClass(GetPersistentSegmentInfoResp.class.getName(), config);
        VerifyClass(GetPersistentSegmentInfoResp.PersistentSegmentInfo.class.getName(), config);
        VerifyClass(GetQuerySegmentInfoResp.class.getName(), config);
        VerifyClass(GetQuerySegmentInfoResp.QuerySegmentInfo.class.getName(), config);
        VerifyClass(ListAliasResp.class.getName(), config);

        // io.milvus/v2/service/vector
        config.assertSetter = false;
        config.assertGetter = false;
        VerifyClass(BoostRanker.class.getName(), config);
        VerifyClass(DecayRanker.class.getName(), config);
        VerifyClass(ModelRanker.class.getName(), config);
        VerifyClass(RRFRanker.class.getName(), config);
        VerifyClass(WeightedRanker.class.getName(), config);
        config.assertSetter = true;
        config.assertGetter = true;

        config.setIgnoredMethods(Arrays.asList("topK", "setTopK", "getTopK", "expr", "setExpr", "getExpr"));
        VerifyClass(AnnSearchReq.class.getName(), config);
        VerifyClass(HybridSearchReq.class.getName(), config);
        VerifyClass(SearchIteratorReq.class.getName(), config);
        VerifyClass(SearchIteratorReqV2.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DeleteReq.class.getName(), config);
        VerifyClass(FunctionScore.class.getName(), config);
        VerifyClass(GetReq.class.getName(), config);
        VerifyClass(InsertReq.class.getName(), config);
        VerifyClass(QueryIteratorReq.class.getName(), config);
        VerifyClass(QueryReq.class.getName(), config);
        VerifyClass(RunAnalyzerReq.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("topK", "setTopK", "getTopK"));
        VerifyClass(SearchReq.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(UpsertReq.class.getName(), config);

        VerifyClass(DeleteResp.class.getName(), config);
        VerifyClass(GetResp.class.getName(), config);
        VerifyClass(InsertResp.class.getName(), config);
        VerifyClass(QueryResp.class.getName(), config);
        VerifyClass(QueryResp.QueryResult.class.getName(), config);
        VerifyClass(RunAnalyzerResp.class.getName(), config);
        VerifyClass(SearchResp.class.getName(), config);
        VerifyClass(SearchResp.SearchResult.class.getName(), config);
        VerifyClass(UpsertResp.class.getName(), config);
    }

    @Test
    void testV1BuilderClasses() {
        CheckConfig config = new CheckConfig();
        config.assertSetter = false;

        // io.milvus/client
        config.setIgnoredMethods(Arrays.asList("withNodeLabels", "setNodeLabels", "getNodeLabels", "setNodeFilter",
                "setFrom", "setTo", "setRequests", "setLimits", "setNodeNum", "setResourceGroupName"));
        VerifyClass(NodeInfo.class.getName(), config);
        VerifyClass(ResourceGroupConfig.class.getName(), config);
        VerifyClass(ResourceGroupLimit.class.getName(), config);
        VerifyClass(ResourceGroupNodeFilter.class.getName(), config);
        config.setIgnoredMethods(Collections.singletonList("setResourceGroupName"));
        VerifyClass(ResourceGroupTransfer.class.getName(), config);
        config.clearIgnoredMethods();

        // io.milvus/param/alias
        VerifyClass(AlterAliasParam.class.getName(), config);
        VerifyClass(CreateAliasParam.class.getName(), config);
        VerifyClass(DropAliasParam.class.getName(), config);
        VerifyClass(ListAliasesParam.class.getName(), config);

        // io.milvus/param/bulkinsert
        config.setIgnoredMethods(Arrays.asList("withOptions", "getOptions"));
        VerifyClass(BulkInsertParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(GetBulkInsertStateParam.class.getName(), config);
        VerifyClass(ListBulkInsertTasksParam.class.getName(), config);

        // io.milvus/param/collection
        config.setIgnoredMethods(Arrays.asList("withProperties", "getProperties"));
        VerifyClass(AlterCollectionParam.class.getName(), config);
        VerifyClass(AlterDatabaseParam.class.getName(), config);
        VerifyClass(CreateDatabaseParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Arrays.asList("withProperties", "getProperties", "build"));
        VerifyClass(CreateCollectionParam.class.getName(), config);
        VerifyClass(CollectionSchemaParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DescribeCollectionParam.class.getName(), config);
        VerifyClass(DescribeDatabaseParam.class.getName(), config);
        VerifyClass(DropCollectionParam.class.getName(), config);
        VerifyClass(DropDatabaseParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("isIsDynamic", "build"));
        VerifyClass(FieldType.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(FlushParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withFlushCollection", "isFlushCollection"));
        VerifyClass(GetCollectionStatisticsParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(GetLoadingProgressParam.class.getName(), config);
        VerifyClass(GetLoadStateParam.class.getName(), config);
        VerifyClass(HasCollectionParam.class.getName(), config);
        VerifyClass(LoadCollectionParam.class.getName(), config);
        VerifyClass(ReleaseCollectionParam.class.getName(), config);
        VerifyClass(RenameCollectionParam.class.getName(), config);
        VerifyClass(ShowCollectionsParam.class.getName(), config);

        // io.milvus/param/control
        VerifyClass(GetCompactionPlansParam.class.getName(), config);
        VerifyClass(GetCompactionStateParam.class.getName(), config);
        VerifyClass(GetFlushAllStateParam.class.getName(), config);
        VerifyClass(GetFlushStateParam.class.getName(), config);
        VerifyClass(GetMetricsParam.class.getName(), config);
        VerifyClass(GetPersistentSegmentInfoParam.class.getName(), config);
        VerifyClass(GetQuerySegmentInfoParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withShardNodes", "isWithShardNodes", "withWithShardNodes"));
        VerifyClass(GetReplicasParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Arrays.asList("getSrcNodeID", "withSrcNodeID", "withDestNodeIDs", "getDestNodeIDs",
                "build"));
        VerifyClass(LoadBalanceParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(ManualCompactParam.class.getName(), config);

        // io.milvus/param/credential
        VerifyClass(CreateCredentialParam.class.getName(), config);
        VerifyClass(DeleteCredentialParam.class.getName(), config);
        VerifyClass(ListCredUsersParam.class.getName(), config);
        VerifyClass(UpdateCredentialParam.class.getName(), config);

        // io.milvus/param/dml
        VerifyClass(io.milvus.param.dml.ranker.RRFRanker.class.getName(), config);
        VerifyClass(io.milvus.param.dml.ranker.WeightedRanker.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withVectors", "withNQ", "getNQ", "withPlType", "getPlType", "build"));
        VerifyClass(AnnSearchParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DeleteParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withSearchRequests", "build"));
        VerifyClass(HybridSearchParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Arrays.asList("withRowCount", "getRowCount", "build"));
        VerifyClass(InsertParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Arrays.asList("withTravelTimestamp", "getTravelTimestamp", "withGracefulTime",
                "getGracefulTime", "withGuaranteeTimestamp", "getGuaranteeTimestamp"));
        VerifyClass(QueryIteratorParam.class.getName(), config);
        VerifyClass(QueryParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withTravelTimestamp", "getTravelTimestamp", "withGracefulTime",
                "getGracefulTime", "withGuaranteeTimestamp", "getGuaranteeTimestamp", "withVectors", "withNQ",
                "getNQ", "withPlType", "getPlType", "build"));
        VerifyClass(SearchParam.class.getName(), config);
        VerifyClass(SearchIteratorParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Collections.singletonList("build"));
        VerifyClass(UpsertParam.class.getName(), config);
        config.clearIgnoredMethods();

        // io.milvus/param/highlevel
        config.setIgnoredMethods(Collections.singletonList("getCollectionNames"));
        VerifyClass(ListCollectionsResponse.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DeleteResponse.class.getName(), config);
        VerifyClass(GetResponse.class.getName(), config);
        VerifyClass(InsertResponse.class.getName(), config);
        VerifyClass(QueryResponse.class.getName(), config);
        config.setIgnoredMethods(Collections.singletonList("getRowRecords"));
        VerifyClass(SearchResponse.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DeleteIdsParam.class.getName(), config);
        VerifyClass(GetIdsParam.class.getName(), config);
        VerifyClass(QuerySimpleParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("withParams", "getParams", "withLimit", "getLimit"));
        VerifyClass(SearchSimpleParam.class.getName(), config);
        config.clearIgnoredMethods();

        // io.milvus/param/index
        config.setIgnoredMethods(Arrays.asList("withProperties", "getProperties"));
        VerifyClass(AlterIndexParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Arrays.asList("withExtraParam", "getExtraParam"));
        VerifyClass(CreateIndexParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DescribeIndexParam.class.getName(), config);
        VerifyClass(DropIndexParam.class.getName(), config);
        VerifyClass(GetIndexBuildProgressParam.class.getName(), config);
        VerifyClass(GetIndexStateParam.class.getName(), config);

        // io.milvus/param/partition
        VerifyClass(CreatePartitionParam.class.getName(), config);
        VerifyClass(DropCollectionParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("isFlushCollection", "withFlushCollection"));
        VerifyClass(GetPartitionStatisticsParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(HasPartitionParam.class.getName(), config);
        VerifyClass(LoadPartitionsParam.class.getName(), config);
        VerifyClass(ReleasePartitionsParam.class.getName(), config);
        config.setIgnoredMethods(Arrays.asList("getShowType", "withShowType"));
        VerifyClass(ShowPartitionsParam.class.getName(), config);
        config.clearIgnoredMethods();

        // io.milvus/param/resourcegroup
        config.setIgnoredMethods(Arrays.asList("withNodeLabels", "setNodeLabels", "getNodeLabels", "setNodeFilter",
                "setFrom", "setTo", "setRequests", "setLimits", "setNodeNum"));
        VerifyClass(CreateResourceGroupParam.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(DescribeResourceGroupParam.class.getName(), config);
        VerifyClass(DropResourceGroupParam.class.getName(), config);
        VerifyClass(ListResourceGroupsParam.class.getName(), config);
        VerifyClass(TransferNodeParam.class.getName(), config);
        VerifyClass(TransferReplicaParam.class.getName(), config);

        // io.milvus/param/role
        VerifyClass(AddUserToRoleParam.class.getName(), config);
        VerifyClass(CreateRoleParam.class.getName(), config);
        VerifyClass(DropRoleParam.class.getName(), config);
        VerifyClass(GrantRolePrivilegeParam.class.getName(), config);
        VerifyClass(RemoveUserFromRoleParam.class.getName(), config);
        VerifyClass(RevokeRolePrivilegeParam.class.getName(), config);
        VerifyClass(SelectGrantForRoleAndObjectParam.class.getName(), config);
        VerifyClass(SelectGrantForRoleParam.class.getName(), config);
        VerifyClass(SelectRoleParam.class.getName(), config);
        VerifyClass(SelectUserParam.class.getName(), config);

        // io.milvus/param
        config.setIgnoredMethods(Arrays.asList("withKeepAliveWithoutCalls", "isKeepAliveWithoutCalls",
                "withClientRequestId", "getClientRequestId", "getRpcDeadlineMs", "withRpcDeadlineMs",
                "getIdleTimeoutMs", "withIdleTimeoutMs", "getConnectTimeoutMs", "withConnectTimeoutMs",
                "getKeepAliveTimeMs", "withKeepAliveTimeMs", "getKeepAliveTimeoutMs", "withKeepAliveTimeoutMs",
                "getUserName", "withUserName", "build"));
        VerifyClass(ConnectParam.class.getName(), config);
        config.clearIgnoredMethods();
        config.setIgnoredMethods(Collections.singletonList("build"));
        VerifyClass(MultiConnectParam.class.getName(), config);
        VerifyClass(QueryNodeSingleSearch.class.getName(), config);
        config.clearIgnoredMethods();
        VerifyClass(RetryParam.class.getName(), config);
        VerifyClass(ServerAddress.class.getName(), config);

        // io.milvus/pool
        config.setIgnoredMethods(Arrays.asList("maxBlockWaitDuration", "getMaxBlockWaitDuration",
                "setMaxBlockWaitDuration", "evictionPollingInterval", "getEvictionPollingInterval",
                "setEvictionPollingInterval", "minEvictableIdleDuration", "getMinEvictableIdleDuration",
                "setMinEvictableIdleDuration"));
        VerifyClass(PoolConfig.class.getName(), config);
        config.clearIgnoredMethods();
    }
}
