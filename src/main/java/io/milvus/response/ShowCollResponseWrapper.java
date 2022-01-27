package io.milvus.response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.ShowCollectionsResponse;

import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>showCollections</code> interface.
 */
public class ShowCollResponseWrapper {
    private final ShowCollectionsResponse response;

    public ShowCollResponseWrapper(@NonNull ShowCollectionsResponse response) {
        this.response = response;
    }

    /**
     * Get information of the collections.
     *
     * @return <code>List<CollectionInfo></code> information array of the collections
     */
    public List<CollectionInfo> getCollectionsInfo() throws IllegalResponseException {
        if (response.getCollectionNamesCount() != response.getCollectionIdsCount()
            || response.getCollectionNamesCount() != response.getCreatedUtcTimestampsCount()) {
            throw new IllegalResponseException("Collection information count doesn't match");
        }

        List<CollectionInfo> results = new ArrayList<>();
        for (int i = 0; i < response.getCollectionNamesCount(); ++i) {
            CollectionInfo info = new CollectionInfo(response.getCollectionNames(i), response.getCollectionIds(i),
                    response.getCreatedUtcTimestamps(i));
            if (response.getInMemoryPercentagesCount() > i) {
                info.SetInMemoryPercentage(response.getInMemoryPercentages(i));
            }
            results.add(info);
        }

        return results;
    }

    /**
     * Get information of one collection by name.
     *
     * @param collectionName collection name to get information
     * @return <code>CollectionInfo</code> information of the collection
     */
    public CollectionInfo getCollectionInfoByName(@NonNull String collectionName) {
        for (int i = 0; i < response.getCollectionNamesCount(); ++i) {
            if ( collectionName.compareTo(response.getCollectionNames(i)) == 0) {
                CollectionInfo info = new CollectionInfo(response.getCollectionNames(i), response.getCollectionIds(i),
                        response.getCreatedUtcTimestamps(i));
                if (response.getInMemoryPercentagesCount() > i) {
                    info.SetInMemoryPercentage(response.getInMemoryPercentages(i));
                }
                return info;
            }
        }

        return null;
    }

    /**
     * Internal-use class to wrap response of <code>ShowCollections</code> interface.
     */
    @Getter
    public static final class CollectionInfo {
        private final String name;
        private final long id;
        private final long utcTimestamp;
        private long inMemoryPercentage = 0;

        public CollectionInfo(String name, long id, long utcTimestamp) {
            this.name = name;
            this.id = id;
            this.utcTimestamp = utcTimestamp;
        }

        public void SetInMemoryPercentage(long inMemoryPercentage) {
            this.inMemoryPercentage = inMemoryPercentage;
        }

        @Override
        public String toString() {
            return "(name: " + getName() + " id: " + getId() + " utcTimestamp: " + getUtcTimestamp() +
                    " inMemoryPercentage: " + getInMemoryPercentage() + ")";
        }
    }

    /**
     * Construct a <code>String</code> by <code>ShowCollResponseWrapper</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Collections{" +
                getCollectionsInfo().toString() +
                '}';
    }
}

