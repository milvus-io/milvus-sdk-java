package io.milvus.client;

import java.util.List;

/** Represents information about a collection */
public class CollectionInfo {
  private final long rowCount;
  private final List<PartitionInfo> partitionInfos;

  CollectionInfo(long rowCount, List<PartitionInfo> partitionInfos) {
    this.rowCount = rowCount;
    this.partitionInfos = partitionInfos;
  }

  public long getRowCount() {
    return rowCount;
  }

  public List<PartitionInfo> getPartitionInfos() {
    return partitionInfos;
  }

  /** Represents information about a single partition in a collection */
  public static class PartitionInfo {
    private final String tag;
    private final long rowCount;
    private final List<SegmentInfo> segmentInfos;

    PartitionInfo(String tag, long rowCount, List<SegmentInfo> segmentInfos) {
      this.tag = tag;
      this.rowCount = rowCount;
      this.segmentInfos = segmentInfos;
    }

    public String getTag() {
      return tag;
    }

    public long getRowCount() {
      return rowCount;
    }

    public List<SegmentInfo> getSegmentInfos() {
      return segmentInfos;
    }

    /** Represents information about a single segment in a partition */
    public static class SegmentInfo {
      private final String segmentName;
      private final long rowCount;
      private final String indexName;
      private final long dataSize;

      SegmentInfo(String segmentName, long rowCount, String indexName, long dataSize) {
        this.segmentName = segmentName;
        this.rowCount = rowCount;
        this.indexName = indexName;
        this.dataSize = dataSize;
      }

      public String getSegmentName() {
        return segmentName;
      }

      public long getRowCount() {
        return rowCount;
      }

      public String getIndexName() {
        return indexName;
      }

      public long getDataSize() {
        return dataSize;
      }
    }
  }
}
