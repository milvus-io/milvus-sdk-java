package io.milvus.v2.service.rbac;

import io.milvus.grpc.*;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class UserService extends BaseService {

    public List<String> listUsers(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "list users";
        ListCredUsersRequest request = ListCredUsersRequest.newBuilder().build();
        ListCredUsersResponse response = blockingStub.listCredUsers(request);
        rpcUtils.handleResponse(title, response.getStatus());
        return response.getUsernamesList();
    }

    public DescribeUserResp describeUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeUserReq request) {
        String title = String.format("describe user %s", request.getUserName());
        // TODO: check user exists
        SelectUserRequest selectUserRequest = SelectUserRequest.newBuilder()
                .setUser(UserEntity.newBuilder().setName(request.getUserName()).build())
                .setIncludeRoleInfo(Boolean.TRUE)
                .build();
        io.milvus.grpc.SelectUserResponse response = blockingStub.selectUser(selectUserRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        DescribeUserResp describeUserResp = DescribeUserResp.builder()
                .roles(response.getResultsList().isEmpty()? null : response.getResultsList().get(0).getRolesList().stream().map(RoleEntity::getName).collect(Collectors.toList()))
                .build();
        return describeUserResp;
    }

    public void createUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateUserReq request) {
        String title = String.format("create user %s", request.getUserName());
        CreateCredentialRequest createCredentialRequest = CreateCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .setPassword(Base64.getEncoder().encodeToString(request.getPassword().getBytes(StandardCharsets.UTF_8)))
                .build();
        Status response = blockingStub.createCredential(createCredentialRequest);
        rpcUtils.handleResponse(title, response);
    }


    public void updatePassword(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdatePasswordReq request) {
        String title = String.format("update password for user %s", request.getUserName());
        UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .setOldPassword(Base64.getEncoder().encodeToString(request.getPassword().getBytes(StandardCharsets.UTF_8)))
                .setNewPassword(Base64.getEncoder().encodeToString(request.getNewPassword().getBytes(StandardCharsets.UTF_8)))
                .build();
        Status response = blockingStub.updateCredential(updateCredentialRequest);
        rpcUtils.handleResponse(title, response);
    }

    public void dropUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropUserReq request) {
        String title = String.format("drop user %s", request.getUserName());
        DeleteCredentialRequest deleteCredentialRequest = DeleteCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .build();
        Status response = blockingStub.deleteCredential(deleteCredentialRequest);
        rpcUtils.handleResponse(title, response);
    }
}
