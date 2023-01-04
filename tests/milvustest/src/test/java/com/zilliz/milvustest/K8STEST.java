package com.zilliz.milvustest;

import com.zilliz.milvustest.util.K8SUtils;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Author yongpeng.li
 * @Date 2023/1/3 17:27
 */
public class K8STEST {

    public static void main(String[] args) throws ApiException, IOException {
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader("src/test/java/resources/config/k8s.config"))).build();
        Configuration.setDefaultApiClient(client);
        CoreV1Api api = new CoreV1Api();
        V1PodList list = api.listPodForAllNamespaces(null,null,null,null,null,null,null,null,null,null);
        StringBuilder str = new StringBuilder();
        for (V1Pod item : list.getItems()) {
            str.append(item.toString());
            str.append("\n");
        }
        System.out.println(str.toString());

    }
}
