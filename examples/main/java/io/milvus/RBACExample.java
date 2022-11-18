package io.milvus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ListCredUsersResponse;
import io.milvus.grpc.SelectRoleResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.role.*;
import org.apache.commons.lang3.Validate;

public class RBACExample {
    private static final MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withAuthorization("root","Milvus")
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    public static R<RpcStatus> createUser(String userName, String password) {
        return milvusClient.createCredential(CreateCredentialParam.newBuilder()
                .withUsername(userName)
                .withPassword(password)
                .build());
    }

    public static R<RpcStatus> grantUserRole(String userName, String roleName) {
        return milvusClient.addUserToRole(AddUserToRoleParam.newBuilder()
                .withUserName(userName)
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> revokeUserRole(String userName, String roleName) {
        return milvusClient.removeUserFromRole(RemoveUserFromRoleParam.newBuilder()
                .withUserName(userName)
                .withRoleName(roleName)
                .build());
    }

    public static R<ListCredUsersResponse> listUsers() {
        return milvusClient.listCredUsers(ListCredUsersParam.newBuilder()
                .build());
    }

    public static R<SelectRoleResponse> selectRole(String roleName) {
        return milvusClient.selectRole(SelectRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> createRole(String roleName) {
        return milvusClient.createRole(CreateRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> dropRole(String roleName) {
        return milvusClient.dropRole(DropRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> grantRolePrivilege(String roleName, String objectType, String objectName, String privilege) {
        return milvusClient.grantRolePrivilege(GrantRolePrivilegeParam.newBuilder()
                .withRoleName(roleName)
                .withObject(objectType)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    }

    public static R<RpcStatus> revokeRolePrivilege(String roleName, String objectType, String objectName, String privilege) {
        return milvusClient.revokeRolePrivilege(RevokeRolePrivilegeParam.newBuilder()
                .withRoleName(roleName)
                .withObject(objectType)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    }


    public static void main(String[] args) {
        // create a role
        R<RpcStatus> resp = createRole("role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "create role fail!");

        //create user
        resp = createUser("user", "pwd123456");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "create user fail!");

        // grant privilege to role.
        // grant object is all collections, grant object type is Collection, and the privilege is CreateCollection
        resp = grantRolePrivilege("role1","Global","*",  "CreateCollection");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "bind privileges to role fail!");

        // bind role to user
        resp = grantUserRole("user", "role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "bind role to user fail!");

        // revoke privilege from role
        resp = revokeRolePrivilege("role1","Global","*",  "CreateCollection");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "revoke privileges to role fail!");

        // list role
        R<SelectRoleResponse> resp1 = selectRole("role1");
        Validate.isTrue(resp1.getStatus() == R.success().getStatus(), "select role information fail!");

        // delete a role
        resp = dropRole("role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "drop role fail!");
    }
}
