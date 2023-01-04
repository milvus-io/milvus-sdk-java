package com.zilliz.milvustest.util;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.credentials.AccessTokenAuthentication;

/**
 * @Author yongpeng.li
 * @Date 2023/1/3 17:23
 */
public class K8SUtils {
    public static ApiClient getApiClient(){

        String master = "https://devops.apiserver.zilliz.cc:6443/";
        String oauthToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1SjZtbE00dTJxUWE3RHhnUUQ3MjVONUVRY3NpWlgzc1FEN09iNVV5d2sifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjo1MjU4NzE2MDU5LCJpYXQiOjE2NTg3MTk2NTksImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJxYSIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJxYS1hZG1pbiIsInVpZCI6ImI3MWRkNmI4LTMxOGUtNGQzNi1hNjE1LWZmZDI1MGQ3NjI3MiJ9fSwibmJmIjoxNjU4NzE5NjU5LCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6cWE6cWEtYWRtaW4ifQ.JwMQJ2bUKjXHSd2brWBKwKGlrtXTgZre1vJZ1j0hy5AK1JzhhC4QSfuU7QDdkksAF4xPPj7DJ31e2Qclvs3F4WviRBr0v8fYRDgEqNpSKdIJ9KGTqJEjD2UwsITJC4_P8sNKtX8yw4-eVitha7Bf2na7eorOQas-ebjGvNf4usUHGQh9eQDfR7DMebUn4dzJfE2ArQn5Ua6dKNkdO-7uQpOuqPIKytEfzIk_ZrWmbWTbzNMaq2z-zb_yd8iTU3sfpMaZLgOAkLtl1XxnDwvFMapJJcPFEqPMVvR98O7BkRkobQ4SngZmwEui9U2I-ann8cS_yjKB4DeajCxCvCoJBw";

        ApiClient apiClient = new ClientBuilder()
                //设置 k8s 服务所在 ip地址
                .setBasePath(master)
                //是否开启 ssl 验证
                .setVerifyingSsl(false)
                //插入访问 连接用的 Token
                .setAuthentication(new AccessTokenAuthentication(oauthToken))
                .build();
        io.kubernetes.client.openapi.Configuration.setDefaultApiClient(apiClient);
        return apiClient;
    }
}
