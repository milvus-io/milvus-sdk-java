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

package io.milvus.server;

import io.grpc.stub.StreamObserver;
import io.milvus.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockMilvusServerImpl extends MilvusServiceGrpc.MilvusServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(MockMilvusServerImpl.class);
    private io.milvus.grpc.ConnectResponse respConnect;
    private io.milvus.grpc.Status respCreateCollection;
    private io.milvus.grpc.DescribeCollectionResponse respDescribeCollection;
    private io.milvus.grpc.Status respDropCollection;
    private io.milvus.grpc.BoolResponse respHasCollection;
    private io.milvus.grpc.GetCollectionStatisticsResponse respGetCollectionStatisticsResponse;
    private io.milvus.grpc.Status respLoadCollection;
    private io.milvus.grpc.Status respReleaseCollection;
    private io.milvus.grpc.ShowCollectionsResponse respShowCollections;
    private io.milvus.grpc.Status respAlterCollection;
    private io.milvus.grpc.Status respCreatePartition;
    private io.milvus.grpc.Status respDropPartition;
    private io.milvus.grpc.BoolResponse respHasPartition;
    private io.milvus.grpc.Status respLoadPartitions;
    private io.milvus.grpc.Status respReleasePartitions;
    private io.milvus.grpc.GetPartitionStatisticsResponse respGetPartitionStatistics;
    private io.milvus.grpc.ShowPartitionsResponse respShowPartitions;
    private io.milvus.grpc.Status respCreateAlias;
    private io.milvus.grpc.Status respDropAlias;
    private io.milvus.grpc.Status respAlterAlias;
    private io.milvus.grpc.ListAliasesResponse respListAliases;
    private io.milvus.grpc.Status respCreateIndex;
    private io.milvus.grpc.DescribeIndexResponse respDescribeIndex;
    private io.milvus.grpc.GetIndexStateResponse respGetIndexState;
    private io.milvus.grpc.GetIndexBuildProgressResponse respGetIndexBuildProgress;
    private io.milvus.grpc.Status respDropIndex;
    private io.milvus.grpc.MutationResult respInsert;
    private io.milvus.grpc.MutationResult respDelete;
    private io.milvus.grpc.ImportResponse respImport;
    private io.milvus.grpc.GetImportStateResponse respImportState;
    private io.milvus.grpc.ListImportTasksResponse respListImportTasks;
    private io.milvus.grpc.SearchResults respSearch;
    private io.milvus.grpc.FlushResponse respFlush;
    private io.milvus.grpc.QueryResults respQuery;
//    private io.milvus.grpc.CalcDistanceResults respCalcDistance;
    private io.milvus.grpc.GetFlushStateResponse respGetFlushState;
    private io.milvus.grpc.GetPersistentSegmentInfoResponse respGetPersistentSegmentInfo;
    private io.milvus.grpc.GetQuerySegmentInfoResponse respGetQuerySegmentInfo;
    private io.milvus.grpc.GetReplicasResponse respGetReplicas;
    private io.milvus.grpc.GetMetricsResponse respGetMetrics;

    private io.milvus.grpc.Status respLoadBalance;
    private io.milvus.grpc.GetCompactionStateResponse respGetCompactionState;
    private io.milvus.grpc.ManualCompactionResponse respManualCompaction;
    private io.milvus.grpc.GetCompactionPlansResponse respGetCompactionPlans;

    private io.milvus.grpc.Status respCreateCredential;
    private io.milvus.grpc.Status respUpdateCredential;
    private io.milvus.grpc.Status respDeleteCredential;
    private io.milvus.grpc.ListCredUsersResponse respListCredUsers;

    private io.milvus.grpc.GetLoadingProgressResponse respGetLoadingProgress;
    private io.milvus.grpc.GetLoadStateResponse respGetLoadState;

    public MockMilvusServerImpl() {
    }

    @Override
    public void connect(io.milvus.grpc.ConnectRequest request,
                        io.grpc.stub.StreamObserver<io.milvus.grpc.ConnectResponse> responseObserver) {
        logger.info("MockServer receive connect() call");

        responseObserver.onNext(respConnect);
        responseObserver.onCompleted();
    }

    public void setConnectResponse(io.milvus.grpc.ConnectResponse resp) {
        respConnect = resp;
    }

    @Override
    public void createCollection(io.milvus.grpc.CreateCollectionRequest request,
                                 io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive createCollection() call");

        responseObserver.onNext(respCreateCollection);
        responseObserver.onCompleted();
    }

    public void setCreateCollectionResponse(io.milvus.grpc.Status resp) {
        respCreateCollection = resp;
    }

    @Override
    public void describeCollection(io.milvus.grpc.DescribeCollectionRequest request,
                                   io.grpc.stub.StreamObserver<io.milvus.grpc.DescribeCollectionResponse> responseObserver) {
        logger.info("MockServer receive describeCollection() call");

        responseObserver.onNext(respDescribeCollection);
        responseObserver.onCompleted();
    }

    public void setDescribeCollectionResponse(io.milvus.grpc.DescribeCollectionResponse resp) {
        respDescribeCollection = resp;
    }

    @Override
    public void dropCollection(io.milvus.grpc.DropCollectionRequest request,
                               io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive dropCollection() call");

        responseObserver.onNext(respDropCollection);
        responseObserver.onCompleted();
    }

    public void setDropCollectionResponse(io.milvus.grpc.Status resp) {
        respDropCollection = resp;
    }

    @Override
    public void getCollectionStatistics(io.milvus.grpc.GetCollectionStatisticsRequest request,
                                        io.grpc.stub.StreamObserver<io.milvus.grpc.GetCollectionStatisticsResponse> responseObserver) {
        logger.info("MockServer receive getCollectionStatistics() call");

        responseObserver.onNext(respGetCollectionStatisticsResponse);
        responseObserver.onCompleted();
    }

    public void setGetCollectionStatisticsResponse(io.milvus.grpc.GetCollectionStatisticsResponse resp) {
        respGetCollectionStatisticsResponse = resp;
    }

    @Override
    public void hasCollection(io.milvus.grpc.HasCollectionRequest request,
                              io.grpc.stub.StreamObserver<io.milvus.grpc.BoolResponse> responseObserver) {
        logger.info("MockServer receive hasCollection() call");

        responseObserver.onNext(respHasCollection);
        responseObserver.onCompleted();
    }

    public void setHasCollectionResponse(io.milvus.grpc.BoolResponse resp) {
        respHasCollection = resp;
    }

    @Override
    public void loadCollection(io.milvus.grpc.LoadCollectionRequest request,
                               io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive loadCollection() call");

        responseObserver.onNext(respLoadCollection);
        responseObserver.onCompleted();
    }

    public void setLoadCollectionResponse(io.milvus.grpc.Status resp) {
        respLoadCollection = resp;
    }

    @Override
    public void releaseCollection(io.milvus.grpc.ReleaseCollectionRequest request,
                                  io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive releaseCollection() call");

        responseObserver.onNext(respReleaseCollection);
        responseObserver.onCompleted();
    }

    public void setReleaseCollectionResponse(io.milvus.grpc.Status resp) {
        respReleaseCollection = resp;
    }

    @Override
    public void showCollections(io.milvus.grpc.ShowCollectionsRequest request,
                                io.grpc.stub.StreamObserver<io.milvus.grpc.ShowCollectionsResponse> responseObserver) {
        logger.info("MockServer receive showCollections() call");

        responseObserver.onNext(respShowCollections);
        responseObserver.onCompleted();
    }

    @Override
    public void alterCollection(io.milvus.grpc.AlterCollectionRequest request,
                                io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive alterCollection() call");

        responseObserver.onNext(respAlterCollection);
        responseObserver.onCompleted();
    }

    public void setShowCollectionsResponse(io.milvus.grpc.ShowCollectionsResponse resp) {
        respShowCollections = resp;
    }

    @Override
    public void createPartition(io.milvus.grpc.CreatePartitionRequest request,
                                io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive createPartition() call");

        responseObserver.onNext(respCreatePartition);
        responseObserver.onCompleted();
    }

    public void setCreatePartitionResponse(io.milvus.grpc.Status resp) {
        respCreatePartition = resp;
    }

    @Override
    public void dropPartition(io.milvus.grpc.DropPartitionRequest request,
                              io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive dropPartition() call");

        responseObserver.onNext(respDropPartition);
        responseObserver.onCompleted();
    }

    public void setDropPartitionResponse(io.milvus.grpc.Status resp) {
        respDropPartition = resp;
    }

    @Override
    public void hasPartition(io.milvus.grpc.HasPartitionRequest request,
                             io.grpc.stub.StreamObserver<io.milvus.grpc.BoolResponse> responseObserver) {
        logger.info("MockServer receive hasPartition() call");

        responseObserver.onNext(respHasPartition);
        responseObserver.onCompleted();
    }

    public void setHasPartitionResponse(io.milvus.grpc.BoolResponse resp) {
        respHasPartition = resp;
    }

    @Override
    public void loadPartitions(io.milvus.grpc.LoadPartitionsRequest request,
                               io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive loadPartitions() call");

        responseObserver.onNext(respLoadPartitions);
        responseObserver.onCompleted();
    }

    public void setLoadPartitionsResponse(io.milvus.grpc.Status resp) {
        respLoadPartitions = resp;
    }

    @Override
    public void releasePartitions(io.milvus.grpc.ReleasePartitionsRequest request,
                                  io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive releasePartitions() call");

        responseObserver.onNext(respReleasePartitions);
        responseObserver.onCompleted();
    }

    public void setReleasePartitionsResponse(io.milvus.grpc.Status resp) {
        respReleasePartitions = resp;
    }

    @Override
    public void getPartitionStatistics(io.milvus.grpc.GetPartitionStatisticsRequest request,
                                       io.grpc.stub.StreamObserver<io.milvus.grpc.GetPartitionStatisticsResponse> responseObserver) {
        logger.info("MockServer receive getPartitionStatistics() call");

        responseObserver.onNext(respGetPartitionStatistics);
        responseObserver.onCompleted();
    }

    public void setGetPartitionStatisticsResponse(io.milvus.grpc.GetPartitionStatisticsResponse resp) {
        respGetPartitionStatistics = resp;
    }

    @Override
    public void showPartitions(io.milvus.grpc.ShowPartitionsRequest request,
                               io.grpc.stub.StreamObserver<io.milvus.grpc.ShowPartitionsResponse> responseObserver) {
        logger.info("MockServer receive showPartitions() call");

        responseObserver.onNext(respShowPartitions);
        responseObserver.onCompleted();
    }

    public void setShowPartitionsResponse(io.milvus.grpc.ShowPartitionsResponse resp) {
        respShowPartitions = resp;
    }

    @Override
    public void createAlias(io.milvus.grpc.CreateAliasRequest request,
                            io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive createAlias() call");

        responseObserver.onNext(respCreateAlias);
        responseObserver.onCompleted();
    }

    public void setCreateAliasResponse(io.milvus.grpc.Status resp) {
        respCreateAlias = resp;
    }

    @Override
    public void dropAlias(io.milvus.grpc.DropAliasRequest request,
                          io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive dropAlias() call");

        responseObserver.onNext(respDropAlias);
        responseObserver.onCompleted();
    }

    public void setDropAliasResponse(io.milvus.grpc.Status resp) {
        respDropAlias = resp;
    }

    @Override
    public void alterAlias(io.milvus.grpc.AlterAliasRequest request,
                           io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive alterAlias() call");

        responseObserver.onNext(respAlterAlias);
        responseObserver.onCompleted();
    }

    public void setAlterAliasResponse(io.milvus.grpc.Status resp) {
        respAlterAlias = resp;
    }

    @Override
    public void listAliases(io.milvus.grpc.ListAliasesRequest request,
                           io.grpc.stub.StreamObserver<io.milvus.grpc.ListAliasesResponse> responseObserver) {
        logger.info("MockServer receive listAliases() call");

        responseObserver.onNext(respListAliases);
        responseObserver.onCompleted();
    }

    public void setListAliasesResponse(io.milvus.grpc.ListAliasesResponse resp) {
        respListAliases = resp;
    }

    @Override
    public void createIndex(io.milvus.grpc.CreateIndexRequest request,
                            io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive createIndex() call");

        responseObserver.onNext(respCreateIndex);
        responseObserver.onCompleted();
    }

    public void setCreateIndexResponse(io.milvus.grpc.Status resp) {
        respCreateIndex = resp;
    }

    @Override
    public void describeIndex(io.milvus.grpc.DescribeIndexRequest request,
                              io.grpc.stub.StreamObserver<io.milvus.grpc.DescribeIndexResponse> responseObserver) {
        logger.info("MockServer receive describeIndex() call");

        responseObserver.onNext(respDescribeIndex);
        responseObserver.onCompleted();
    }

    public void setDescribeIndexResponse(io.milvus.grpc.DescribeIndexResponse resp) {
        respDescribeIndex = resp;
    }

    @Override
    public void getIndexState(io.milvus.grpc.GetIndexStateRequest request,
                              io.grpc.stub.StreamObserver<io.milvus.grpc.GetIndexStateResponse> responseObserver) {
        logger.info("MockServer receive getIndexState() call");

        responseObserver.onNext(respGetIndexState);
        responseObserver.onCompleted();
    }

    public void setGetIndexStateResponse(io.milvus.grpc.GetIndexStateResponse resp) {
        respGetIndexState = resp;
    }

    @Override
    public void getIndexBuildProgress(io.milvus.grpc.GetIndexBuildProgressRequest request,
                                      io.grpc.stub.StreamObserver<io.milvus.grpc.GetIndexBuildProgressResponse> responseObserver) {
        logger.info("MockServer receive getIndexBuildProgress() call");

        responseObserver.onNext(respGetIndexBuildProgress);
        responseObserver.onCompleted();
    }

    public void setGetIndexBuildProgressResponse(io.milvus.grpc.GetIndexBuildProgressResponse resp) {
        respGetIndexBuildProgress = resp;
    }

    @Override
    public void dropIndex(io.milvus.grpc.DropIndexRequest request,
                          io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive dropIndex() call");

        responseObserver.onNext(respDropIndex);
        responseObserver.onCompleted();
    }

    public void setDropIndexResponse(io.milvus.grpc.Status resp) {
        respDropIndex = resp;
    }

    @Override
    public void insert(io.milvus.grpc.InsertRequest request,
                       io.grpc.stub.StreamObserver<io.milvus.grpc.MutationResult> responseObserver) {
        logger.info("MockServer receive insert() call");

        responseObserver.onNext(respInsert);
        responseObserver.onCompleted();
    }

    public void setInsertResponse(io.milvus.grpc.MutationResult resp) {
        respInsert = resp;
    }

    @Override
    public void delete(io.milvus.grpc.DeleteRequest request,
                       io.grpc.stub.StreamObserver<io.milvus.grpc.MutationResult> responseObserver) {
        logger.info("MockServer receive delete() call");

        responseObserver.onNext(respDelete);
        responseObserver.onCompleted();
    }

    @Override
    public void import_(io.milvus.grpc.ImportRequest request,
                       io.grpc.stub.StreamObserver<io.milvus.grpc.ImportResponse> responseObserver) {
        logger.info("MockServer receive import() call");

        responseObserver.onNext(respImport);
        responseObserver.onCompleted();
    }

    @Override
    public void getImportState(io.milvus.grpc.GetImportStateRequest request,
                        io.grpc.stub.StreamObserver<io.milvus.grpc.GetImportStateResponse> responseObserver) {
        logger.info("MockServer receive getImportState() call");

        responseObserver.onNext(respImportState);
        responseObserver.onCompleted();
    }

    @Override
    public void listImportTasks(io.milvus.grpc.ListImportTasksRequest request,
                               io.grpc.stub.StreamObserver<io.milvus.grpc.ListImportTasksResponse> responseObserver) {
        logger.info("MockServer receive listImportTasks() call");

        responseObserver.onNext(respListImportTasks);
        responseObserver.onCompleted();
    }

    public void setDeleteResponse(io.milvus.grpc.MutationResult resp) {
        respDelete = resp;
    }

    @Override
    public void search(io.milvus.grpc.SearchRequest request,
                       io.grpc.stub.StreamObserver<io.milvus.grpc.SearchResults> responseObserver) {
        logger.info("MockServer receive search() call");

        responseObserver.onNext(respSearch);
        responseObserver.onCompleted();
    }

    @Override
    public void hybridSearch(io.milvus.grpc.HybridSearchRequest request,
                             io.grpc.stub.StreamObserver<io.milvus.grpc.SearchResults> responseObserver) {
        logger.info("MockServer receive hybridSearch() call");

        responseObserver.onNext(respSearch);
        responseObserver.onCompleted();
    }

    public void setSearchResponse(io.milvus.grpc.SearchResults resp) {
        respSearch = resp;
    }

    @Override
    public void flush(io.milvus.grpc.FlushRequest request,
                      io.grpc.stub.StreamObserver<io.milvus.grpc.FlushResponse> responseObserver) {
        logger.info("MockServer receive flush() call");

        responseObserver.onNext(respFlush);
        responseObserver.onCompleted();
    }

    public void setFlushResponse(io.milvus.grpc.FlushResponse resp) {
        respFlush = resp;
    }

    @Override
    public void query(io.milvus.grpc.QueryRequest request,
                      io.grpc.stub.StreamObserver<io.milvus.grpc.QueryResults> responseObserver) {
        logger.info("MockServer receive query() call");

        responseObserver.onNext(respQuery);
        responseObserver.onCompleted();
    }

    public void setQueryResponse(io.milvus.grpc.QueryResults resp) {
        respQuery = resp;
    }

//    @Override
//    public void calcDistance(io.milvus.grpc.CalcDistanceRequest request,
//                             io.grpc.stub.StreamObserver<io.milvus.grpc.CalcDistanceResults> responseObserver) {
//        logger.info("MockServer receive calcDistance() call");
//
//        responseObserver.onNext(respCalcDistance);
//        responseObserver.onCompleted();
//    }
//
//    public void setCalcDistanceResponse(io.milvus.grpc.CalcDistanceResults resp) {
//        respCalcDistance = resp;
//    }

    @Override
    public void getFlushState(io.milvus.grpc.GetFlushStateRequest request,
                                         io.grpc.stub.StreamObserver<io.milvus.grpc.GetFlushStateResponse> responseObserver) {
        logger.info("MockServer receive getFlushState() call");

        responseObserver.onNext(respGetFlushState);
        responseObserver.onCompleted();
    }

    public void setGetFlushStateResponse(io.milvus.grpc.GetFlushStateResponse resp) {
        respGetFlushState = resp;
    }

    @Override
    public void getPersistentSegmentInfo(io.milvus.grpc.GetPersistentSegmentInfoRequest request,
                                         io.grpc.stub.StreamObserver<io.milvus.grpc.GetPersistentSegmentInfoResponse> responseObserver) {
        logger.info("MockServer receive getPersistentSegmentInfo() call");

        responseObserver.onNext(respGetPersistentSegmentInfo);
        responseObserver.onCompleted();
    }

    public void setGetPersistentSegmentInfoResponse(io.milvus.grpc.GetPersistentSegmentInfoResponse resp) {
        respGetPersistentSegmentInfo = resp;
    }

    @Override
    public void getQuerySegmentInfo(io.milvus.grpc.GetQuerySegmentInfoRequest request,
                                    io.grpc.stub.StreamObserver<io.milvus.grpc.GetQuerySegmentInfoResponse> responseObserver) {
        logger.info("MockServer receive getQuerySegmentInfo() call");

        responseObserver.onNext(respGetQuerySegmentInfo);
        responseObserver.onCompleted();
    }

    public void setGetQuerySegmentInfoResponse(io.milvus.grpc.GetQuerySegmentInfoResponse resp) {
        respGetQuerySegmentInfo = resp;
    }

    @Override
    public void getReplicas(io.milvus.grpc.GetReplicasRequest request,
                                    io.grpc.stub.StreamObserver<io.milvus.grpc.GetReplicasResponse> responseObserver) {
        logger.info("MockServer receive getReplicas() call");

        responseObserver.onNext(respGetReplicas);
        responseObserver.onCompleted();
    }

    public void setGetReplicasResponse(io.milvus.grpc.GetReplicasResponse resp) {
        respGetReplicas = resp;
    }

    @Override
    public void getMetrics(io.milvus.grpc.GetMetricsRequest request,
                           io.grpc.stub.StreamObserver<io.milvus.grpc.GetMetricsResponse> responseObserver) {
        logger.info("MockServer receive getMetrics() call");

        responseObserver.onNext(respGetMetrics);
        responseObserver.onCompleted();
    }

    public void setGetMetricsResponse(io.milvus.grpc.GetMetricsResponse resp) {
        respGetMetrics = resp;
    }

    @Override
    public void loadBalance(io.milvus.grpc.LoadBalanceRequest request,
                            io.grpc.stub.StreamObserver<io.milvus.grpc.Status> responseObserver) {
        logger.info("MockServer receive getMetrics() call");

        responseObserver.onNext(respLoadBalance);
        responseObserver.onCompleted();
    }

    public void setLoadBalanceResponse(io.milvus.grpc.Status resp) {
        respLoadBalance = resp;
    }

    @Override
    public void getCompactionState(io.milvus.grpc.GetCompactionStateRequest request,
                                   io.grpc.stub.StreamObserver<io.milvus.grpc.GetCompactionStateResponse> responseObserver) {
        logger.info("MockServer receive getMetrics() call");

        responseObserver.onNext(respGetCompactionState);
        responseObserver.onCompleted();
    }

    public void setGetCompactionStateResponse(io.milvus.grpc.GetCompactionStateResponse resp) {
        respGetCompactionState = resp;
    }

    @Override
    public void manualCompaction(io.milvus.grpc.ManualCompactionRequest request,
                                 io.grpc.stub.StreamObserver<io.milvus.grpc.ManualCompactionResponse> responseObserver) {
        logger.info("MockServer receive getMetrics() call");

        responseObserver.onNext(respManualCompaction);
        responseObserver.onCompleted();
    }

    public void setManualCompactionResponse(io.milvus.grpc.ManualCompactionResponse resp) {
        respManualCompaction = resp;
    }

    @Override
    public void getCompactionStateWithPlans(io.milvus.grpc.GetCompactionPlansRequest request,
                                            io.grpc.stub.StreamObserver<io.milvus.grpc.GetCompactionPlansResponse> responseObserver) {
        logger.info("MockServer receive getMetrics() call");

        responseObserver.onNext(respGetCompactionPlans);
        responseObserver.onCompleted();
    }

    public void setGetCompactionPlansResponse(io.milvus.grpc.GetCompactionPlansResponse resp) {
        respGetCompactionPlans = resp;
    }

    @Override
    public void createCredential(CreateCredentialRequest request, StreamObserver<Status> responseObserver) {
        logger.info("MockServer receive createCredential() call");

        responseObserver.onNext(respCreateCredential);
        responseObserver.onCompleted();
    }

    public void setRespCreateCredential(Status respCreateCredential) {
        this.respCreateCredential = respCreateCredential;
    }

    @Override
    public void updateCredential(UpdateCredentialRequest request, StreamObserver<Status> responseObserver) {
        logger.info("MockServer receive updateCredential() call");

        responseObserver.onNext(respUpdateCredential);
        responseObserver.onCompleted();
    }

    public void setRespUpdateCredential(Status respUpdateCredential) {
        this.respUpdateCredential = respUpdateCredential;
    }

    @Override
    public void deleteCredential(DeleteCredentialRequest request, StreamObserver<Status> responseObserver) {
        logger.info("MockServer receive deleteCredential() call");

        responseObserver.onNext(respDeleteCredential);
        responseObserver.onCompleted();
    }

    public void setRespDeleteCredential(Status respDeleteCredential) {
        this.respDeleteCredential = respDeleteCredential;
    }

    @Override
    public void listCredUsers(ListCredUsersRequest request, StreamObserver<ListCredUsersResponse> responseObserver) {
        logger.info("MockServer receive listCredUsers() call");

        responseObserver.onNext(respListCredUsers);
        responseObserver.onCompleted();
    }

    public void setRespListCredUsers(ListCredUsersResponse respListCredUsers) {
        this.respListCredUsers = respListCredUsers;
    }

    @Override
    public void getLoadingProgress(GetLoadingProgressRequest request, StreamObserver<GetLoadingProgressResponse> responseObserver) {
        logger.info("MockServer receive getLoadingProgress() call");

        responseObserver.onNext(respGetLoadingProgress);
        responseObserver.onCompleted();
    }

    public void setGetLoadingProgress(GetLoadingProgressResponse respGetLoadingProgress) {
        this.respGetLoadingProgress = respGetLoadingProgress;
    }

    @Override
    public void getLoadState(GetLoadStateRequest request, StreamObserver<GetLoadStateResponse> responseObserver) {
        logger.info("MockServer receive getLoadState() call");

        responseObserver.onNext(respGetLoadState);
        responseObserver.onCompleted();
    }

    public void setGetLoadingProgress(GetLoadStateResponse respGetLoadState) {
        this.respGetLoadState = respGetLoadState;
    }
}
