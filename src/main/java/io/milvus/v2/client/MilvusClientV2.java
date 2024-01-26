package io.milvus.v2.client;

import io.grpc.ManagedChannel;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.service.collection.CollectionService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.PartitionService;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.rbac.RoleService;
import io.milvus.v2.service.rbac.UserService;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.utility.UtilityService;
import io.milvus.v2.service.utility.request.AlterAliasReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.utils.ClientUtils;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class MilvusClientV2 {
    private static final Logger logger = LoggerFactory.getLogger(MilvusClientV2.class);
    private ManagedChannel channel;
    @Setter
    private MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final ClientUtils clientUtils = new ClientUtils();
    private final CollectionService collectionService = new CollectionService();
    private final IndexService indexService = new IndexService();
    private final VectorService vectorService = new VectorService();
    private final PartitionService partitionService = new PartitionService();
    private final UserService userService = new UserService();
    private final RoleService roleService = new RoleService();
    private final UtilityService utilityService = new UtilityService();
    private ConnectConfig connectConfig;

    /**
     * Creates a Milvus client instance.
     * @param connectConfig Milvus server connection configuration
     */
    public MilvusClientV2(ConnectConfig connectConfig) {
        if (connectConfig != null) {
            connect(connectConfig);
        }
    }
    /**
     * connect to Milvus server
     *
     * @param connectConfig Milvus server connection configuration
     */
    private void connect(ConnectConfig connectConfig){
        this.connectConfig = connectConfig;
        try {
            if(this.channel != null) {
                // close channel first
                close(3);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        channel = clientUtils.getChannel(connectConfig);

        if (connectConfig.getRpcDeadlineMs() > 0) {
            blockingStub =  MilvusServiceGrpc.newBlockingStub(channel).withWaitForReady()
                    .withDeadlineAfter(connectConfig.getRpcDeadlineMs(), TimeUnit.MILLISECONDS);
        }else {
            blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
        }

        if (connectConfig.getDatabaseName() != null) {
            // check if database exists
            clientUtils.checkDatabaseExist(this.blockingStub, connectConfig.getDatabaseName());
        }
    }

    /**
     * use Database
     * @param dbName databaseName
     */
    public void useDatabase(@NonNull String dbName) {
        // check if database exists
        clientUtils.checkDatabaseExist(this.blockingStub, dbName);
        try {
            this.connectConfig.setDatabaseName(dbName);
            this.close(3);
            this.connect(this.connectConfig);
        }catch (InterruptedException e){
            logger.error("close connect error");
        }
    }

    //Collection Operations
    /**
     * Fast Creates a collection in Milvus.
     *
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createCollection(CreateCollectionReq request) {
        return collectionService.createCollection(this.blockingStub, request);
    }

    /**
     * Creates a collection with Schema in Milvus.
     *
     * @param request create collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createCollectionWithSchema(CreateCollectionWithSchemaReq request) {
        return collectionService.createCollectionWithSchema(this.blockingStub, request);
    }

    /**
     * list milvus collections
     *
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<ListCollectionsResp> listCollections(){
        return collectionService.listCollections(this.blockingStub);
    }

    /**
     * Drops a collection in Milvus.
     *
     * @param request drop collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropCollection(DropCollectionReq request){
        return collectionService.dropCollection(this.blockingStub, request);
    }
    /**
     * Checks whether a collection exists in Milvus.
     *
     * @param request has collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<Boolean> hasCollection(HasCollectionReq request){
        return collectionService.hasCollection(this.blockingStub, request);
    }
    /**
     * Gets the collection info in Milvus.
     *
     * @param request describe collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<DescribeCollectionResp> describeCollection(DescribeCollectionReq request){
        return collectionService.describeCollection(this.blockingStub, request);
    }
    /**
     * get collection stats for a collection in Milvus.
     *
     * @param request get collection stats request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
//    public R<GetCollectionStatsResp> getCollectionStats(GetCollectionStatsReq request){
//        return collectionService.getCollectionStats(this.blockingStub, request);
//    }
    /**
     * rename collection in a collection in Milvus.
     *
     * @param request rename collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> renameCollection(RenameCollectionReq request){
        return collectionService.renameCollection(this.blockingStub, request);
    }
    /**
     * Loads a collection into memory in Milvus.
     *
     * @param request load collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> loadCollection(LoadCollectionReq request){
        return collectionService.loadCollection(this.blockingStub, request);
    }
    /**
     * Releases a collection from memory in Milvus.
     *
     * @param request release collection request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> releaseCollection(ReleaseCollectionReq request){
        return collectionService.releaseCollection(this.blockingStub, request);
    }
    /**
     * Checks whether a collection is loaded in Milvus.
     *
     * @param request get load state request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<Boolean> getLoadState(GetLoadStateReq request){
        return collectionService.getLoadState(this.blockingStub, request);
    }

    //Index Operations
    /**
     * Creates an index for a specified field in a collection in Milvus.
     *
     * @param request create index request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createIndex(CreateIndexReq request){
        return indexService.createIndex(this.blockingStub, request);
    }
    /**
     * Drops an index for a specified field in a collection in Milvus.
     *
     * @param request drop index request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropIndex(DropIndexReq request){
        return indexService.dropIndex(this.blockingStub, request);
    }
    /**
     * Checks whether an index exists for a specified field in a collection in Milvus.
     *
     * @param request describe index request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<DescribeIndexResp> describeIndex(DescribeIndexReq request){
        return indexService.describeIndex(this.blockingStub, request);
    }

    // Vector Operations

    /**
     * Inserts vectors into a collection in Milvus.
     *
     * @param request insert request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> insert(InsertReq request){
        return vectorService.insert(this.blockingStub, request);
    }
    /**
     * Upsert vectors into a collection in Milvus.
     *
     * @param request upsert request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> upsert(UpsertReq request){
        return vectorService.upsert(this.blockingStub, request);
    }
    /**
     * Deletes vectors in a collection in Milvus.
     *
     * @param request delete request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> delete(DeleteReq request){
        return vectorService.delete(this.blockingStub, request);
    }
    /**
     * Gets vectors in a collection in Milvus.
     *
     * @param request get request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<GetResp> get(GetReq request){
        return vectorService.get(this.blockingStub, request);
    }

    /**
     * Queries vectors in a collection in Milvus.
     *
     * @param request query request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<QueryResp> query(QueryReq request){
        return vectorService.query(this.blockingStub, request);
    }
    /**
     * Searches vectors in a collection in Milvus.
     *
     * @param request search request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<SearchResp> search(SearchReq request){
        return vectorService.search(this.blockingStub, request);
    }

    // Partition Operations
    /**
     * Creates a partition in a collection in Milvus.
     *
     * @param request create partition request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createPartition(CreatePartitionReq request) {
        return partitionService.createPartition(this.blockingStub, request);
    }

    /**
     * Drops a partition in a collection in Milvus.
     *
     * @param request drop partition request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropPartition(DropPartitionReq request) {
        return partitionService.dropPartition(this.blockingStub, request);
    }

    /**
     * Checks whether a partition exists in a collection in Milvus.
     *
     * @param request has partition request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<Boolean> hasPartition(HasPartitionReq request) {
        return partitionService.hasPartition(this.blockingStub, request);
    }

    /**
     * Lists all partitions in a collection in Milvus.
     *
     * @param request list partitions request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<List<String>> listPartitions(ListPartitionsReq request) {
        return partitionService.listPartitions(this.blockingStub, request);
    }

    /**
     * Loads partitions in a collection in Milvus.
     *
     * @param request load partitions request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> loadPartitions(LoadPartitionsReq request) {
        return partitionService.loadPartitions(this.blockingStub, request);
    }
    /**
     * Releases partitions in a collection in Milvus.
     *
     * @param request release partitions request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> releasePartitions(ReleasePartitionsReq request) {
        return partitionService.releasePartitions(this.blockingStub, request);
    }
    // rbac operations
    // user operations
    /**
     * list users
     *
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<List<String>> listUsers(){
        return userService.listUsers(this.blockingStub);
    }
    /**
     * describe user
     *
     * @param request describe user request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<DescribeUserResp> describeUser(DescribeUserReq request){
        return userService.describeUser(this.blockingStub, request);
    }
    /**
     * create user
     *
     * @param request create user request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createUser(CreateUserReq request){
        return userService.createUser(this.blockingStub, request);
    }
    /**
     * change password
     *
     * @param request change password request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> updatePassword(UpdatePasswordReq request) {
        return userService.updatePassword(this.blockingStub, request);
    }
    /**
     * drop user
     *
     * @param request drop user request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropUser(DropUserReq request){
        return userService.dropUser(this.blockingStub, request);
    }
    // role operations
    /**
     * list roles
     *
     * @return {status:result code, data:List<String>{msg: result message}}
     */
    public R<List<String>> listRoles() {
        return roleService.listRoles(this.blockingStub);
    }
    /**
     * describe role
     *
     * @param request describe role request
     * @return {status:result code, data:DescribeRoleResp{msg: result message}}
     */
    public R<DescribeRoleResp> describeRole(DescribeRoleReq request) {
        return roleService.describeRole(this.blockingStub, request);
    }
    /**
     * create role
     *
     * @param request create role request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createRole(CreateRoleReq request) {
        return roleService.createRole(this.blockingStub, request);
    }
    /**
     * drop role
     *
     * @param request drop role request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropRole(DropRoleReq request) {
        return roleService.dropRole(this.blockingStub, request);
    }
    /**
     * grant privilege
     *
     * @param request grant privilege request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> grantPrivilege(GrantPrivilegeReq request) {
        return roleService.grantPrivilege(this.blockingStub, request);
    }
    /**
     * revoke privilege
     *
     * @param request revoke privilege request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> revokePrivilege(RevokePrivilegeReq request) {
        return roleService.revokePrivilege(this.blockingStub, request);
    }
    /**
     * grant role
     *
     * @param request grant role request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> grantRole(GrantRoleReq request) {
        return roleService.grantRole(this.blockingStub, request);
    }
    /**
     * revoke role
     *
     * @param request revoke role request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> revokeRole(RevokeRoleReq request) {
        return roleService.revokeRole(this.blockingStub, request);
    }

    // Utility Operations

    /**
     * create aliases
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> createAlias(CreateAliasReq request) {
        return utilityService.createAlias(this.blockingStub, request);
    }
    /**
     * drop aliases
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> dropAlias(DropAliasReq request) {
        return utilityService.dropAlias(this.blockingStub, request);
    }
    /**
     * alter aliases
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> alterAlias(AlterAliasReq request) {
        return utilityService.alterAlias(this.blockingStub, request);
    }
    /**
     * flush collection
     * @param request flush request
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    public R<RpcStatus> flush(FlushReq request) {
        return utilityService.flush(this.blockingStub, request);
    }
    /**
     * close client
     *
     * @param maxWaitSeconds max wait seconds
     */
    public void close(long maxWaitSeconds) throws InterruptedException {
        if(channel!= null){
            channel.shutdownNow();
            channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
        }
    }
}
