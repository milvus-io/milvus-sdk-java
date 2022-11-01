package com.zilliz.milvustest.businessflow;

import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.SearchParam;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @Author yongpeng.li
 * @Date 2022/9/27 10:55
 */
public class PerformanceTest2 {

    static  MilvusServiceClient milvusClient =
            new MilvusServiceClient(
                    ConnectParam.newBuilder()
                            .withHost(
                                    System.getProperty("milvusHost") == null
                                            ? PropertyFilesUtil.getRunValue("milvusHost")
                                            : System.getProperty("milvusHost"))
                            .withPort(
                                    Integer.parseInt(
                                            System.getProperty("milvusPort") == null
                                                    ? PropertyFilesUtil.getRunValue("milvusPort")
                                                    : System.getProperty("milvusPort")))
                            .withAuthorization("root","Z5:&RCg%d(MQ[gLnPT:tV!nJaJAdp@:$")
                            .withSecure(true)
                            .build());


    @Test
    public void concurrentSearch2() {

        milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("book").build());
        int dim = 128;
        Random ran = new Random();
        final Integer SEARCH_K = 2; // TopK
        final String SEARCH_PARAM = "{\"nprobe\":1}"; // Params
        List<String> search_output_fields = Arrays.asList("book_id", "word_count");

        // concurrent search
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        ArrayList<Future> list = new ArrayList<>();
        for (int j = 0; j < 50; j++) {
            Callable callable =
                    () -> {
                        List<Object> results = new ArrayList<>();
                        for (int i = 0; i < 10000; i++) {
                            List<Float> floatList = new ArrayList<>();
                            for (int k = 0; k < dim; ++k) {
                                floatList.add(ran.nextFloat());
                            }
                            List<List<Float>> search_vectors = Arrays.asList(floatList);
                            SearchParam searchParam =
                                    SearchParam.newBuilder()
                                            .withCollectionName("book")
                                            .withMetricType(MetricType.L2)
                                            .withOutFields(search_output_fields)
                                            .withTopK(SEARCH_K)
                                            .withVectors(search_vectors)
                                            .withVectorFieldName("book_intro")
                                            .withRoundDecimal(4)
                                            .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                                            .withParams(SEARCH_PARAM)
                                            .build();
                            R<SearchResults> search = milvusClient.search(searchParam);
                            results.add(search.getStatus());
                        }
                        return results;
                    };
            Future future = executorService.submit(callable);
            list.add(future);
        }
        executorService.shutdown();

        int sucNUm=0;
        int failNum=0;
        // 遍历future
        for (Future future : list) {
            try {
                if(future.get().toString().equals("0")){
                    sucNUm+=1;
                }else{
                    failNum+=1;
                }
                System.out.println(future);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

