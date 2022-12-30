/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.archive;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.informationcenter.PoolInfo;
import py.instance.Group;
import py.io.qos.MigrationStrategy;

/**
 * this class stores the meta data for the whole archive.
 *
 * <p>Note that ArchiveMataDataWriter is used to persist this object with other information to a
 * file/disk.
 */
public class RawArchiveMetadata extends ArchiveMetadata {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RawArchiveMetadata.class);
  // offset of page indexer
  protected long pageIndexerOffset;
  private Group group;
  // Page Size
  private long segmentUnitSize;
  // max segment unit count that archive can hold.
  // The value is (storage size - overhead) / segment size
  private int maxSegUnitCount;
  // size of page indexers' region
  private long pageIndexersRegionSize;
  // offset of segment unit data begin
  private long segmentUnitDataOffset;
  // virtual offset in archive for segment unit logical address
  private long virtualSegmentUnitDataOffset;
  // offset of segment unit metadata begin
  private long segmentUnitMetadataOffset;
  // offset of brick metadata begin
  private long brickMetadataOffset;
  // archive logical free space
  private long logicalFreeSpace;

  private Long storagePoolId;

  /**
   * add for infocenter.
   */
  private Long freeSpace;

  @JsonIgnore
  private Long usedSpace = 0L;

  private int freeFlexibleSegmentUnitCount;

  private int arbiterLimit;

  private int flexiableLimit;

  // If an archive is overloaded, it means there are too many archive units on this archive, we need
  // to migrate some.
  private boolean overloaded;
  private long usedRatio;

  private int weight;

  // page migration info report to infocenter
  @JsonIgnore
  private int totalPageToMigrate;
  @JsonIgnore
  private int alreadyMigratedPage;
  @JsonIgnore
  private int migrationSpeed;

  // page migration info receive from infocenter
  @JsonIgnore
  private long maxMigrationSpeed;

  @JsonIgnore
  private MigrationStrategy migrationStrategy;

  @JsonIgnore
  private BlockingQueue<SegId> migrateFailedSegIdList = new LinkedBlockingQueue<>();

  @JsonIgnore
  private int dataSizeMb;

  public RawArchiveMetadata() {
    this.segmentUnitSize = 0;
    this.group = null;
    this.storagePoolId = null;
    this.migrationStrategy = MigrationStrategy.Smart;
    this.maxMigrationSpeed = Long.MAX_VALUE;
  }

  public RawArchiveMetadata(ArchiveMetadata archiveMetadataBase) {
    super(archiveMetadataBase);
    this.segmentUnitSize = 0;
    this.group = null;
    this.storagePoolId = null;
    this.migrationStrategy = MigrationStrategy.Smart;
    this.maxMigrationSpeed = Long.MAX_VALUE;
  }

  public long getUsedRatio() {
    return usedRatio;
  }

  public void setUsedRatio(long usedRatio) {
    this.usedRatio = usedRatio;
  }

  public long getSegmentUnitSize() {
    return segmentUnitSize;
  }

  public void setSegmentUnitSize(long segmentUnitSize) {
    this.segmentUnitSize = segmentUnitSize;
  }

  @JsonIgnore
  public int getDataSizeMb() {
    return dataSizeMb;
  }

  @JsonIgnore
  public void setDataSizeMb(int dataSizeMb) {
    this.dataSizeMb = dataSizeMb;
  }

  @Override
  public String toString() {
    return "RawArchiveMetadata{super=" + super.toString() + ", group=" + group
        + ", segmentUnitSize="
        + segmentUnitSize + ", maxSegUnitCount=" + maxSegUnitCount + ", pageIndexersRegionSize="
        + pageIndexersRegionSize + ", segmentUnitDataOffset=" + segmentUnitDataOffset
        + ", pageIndexerOffset="
        + pageIndexerOffset + ", logicalFreeSpace=" + logicalFreeSpace
        + ", storagePoolId=" + storagePoolId + ", freeSpace=" + freeSpace + ", overloaded="
        + overloaded
        + ", usedRatio=" + usedRatio
        + ", totalPageToMigrate=" + totalPageToMigrate
        + ", alreadyMigratedPage=" + alreadyMigratedPage
        + ", migrationSpeed=" + migrationSpeed
        + ", maxMigrationSpeed=" + maxMigrationSpeed
        + ", migrationStrategy=" + migrationStrategy
        + ", arbiterLimit=" + arbiterLimit
        + ", flexiableLimit=" + flexiableLimit
        + ", weight=" + weight
        + ", dataSizeMb=" + dataSizeMb
        + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RawArchiveMetadata other = (RawArchiveMetadata) obj;
    if (super.equals(obj)) {
      return true;
    }
    return false;
  }

  public void copy(RawArchiveMetadata srcMetadata) {
    super.copy(srcMetadata);

    if (srcMetadata.segmentUnitSize != 0) {
      this.segmentUnitSize = srcMetadata.segmentUnitSize;
    }
    if (srcMetadata.group != null) {
      this.group = srcMetadata.group;
    }

    if (srcMetadata.storagePoolId != null) {
      this.storagePoolId = srcMetadata.storagePoolId;
    }
  }

  public boolean addMigrateFailedSegId(SegId segId) {
    if (logger.isDebugEnabled()) {
      logger.debug("Add migrate failed segId:{} to archivemetadata:{}", segId, this);
    }

    boolean result = migrateFailedSegIdList.offer(segId);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Add migrate failed segId:{} success to archivemetadata:{} migrateFailedSegIdList size"
              + " is {}",
          segId, this, migrateFailedSegIdList.size());
    }

    return result;
  }

  public List<SegId> drainAllMigrateFailedSegIds() {
    if (migrateFailedSegIdList.size() > 0) {
      ArrayList<SegId> segIds = new ArrayList<>();
      migrateFailedSegIdList.drainTo(segIds);

      logger.debug("Drain all migrate failed segIds:{} from archivemetadata:{}", segIds, this);

      return segIds;
    } else {
      logger.debug("Drain all migrate failed segIds:[] from archivemetadata:{}", this);
      return new ArrayList<>();
    }
  }

  public Group getGroup() {
    return group;
  }

  public void setGroup(Group group) {
    this.group = group;
  }

  public int getMaxSegUnitCount() {
    return maxSegUnitCount;
  }

  public void setMaxSegUnitCount(int segUnitCount) {
    this.maxSegUnitCount = segUnitCount;
  }

  public long getPageIndexerOffset() {
    return pageIndexerOffset;
  }

  public void setPageIndexerOffset(long pageIndexerOffset) {
    this.pageIndexerOffset = pageIndexerOffset;
  }

  public long getSegmentUnitDataOffset() {
    return segmentUnitDataOffset;
  }

  public void setSegmentUnitDataOffset(long segmentUnitDataOffset) {
    this.segmentUnitDataOffset = segmentUnitDataOffset;
  }

  public long getLogicalFreeSpace() {
    return logicalFreeSpace;
  }

  // TODO
  // set metrics in set function temporary due to archive.java resource can't open, it will set in
  // future
  public void setLogicalFreeSpace(long logicalFreeSpace) {
    this.logicalFreeSpace = logicalFreeSpace;
  }

  public Long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(Long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public boolean isOverloaded() {
    return overloaded;
  }

  public void setOverloaded(boolean overloaded) {
    this.overloaded = overloaded;
  }

  public long getPageIndexersRegionSize() {
    return pageIndexersRegionSize;
  }

  public void setPageIndexersRegionSize(long pageIndexersRegionSize) {
    this.pageIndexersRegionSize = pageIndexersRegionSize;
  }

  public int getFreeFlexibleSegmentUnitCount() {
    return freeFlexibleSegmentUnitCount;
  }

  public void setFreeFlexibleSegmentUnitCount(int freeFlexibleSegmentUnitCount) {
    this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  /**
   * determine if free to add to storage pool determine null is history problem.
   */
  @JsonIgnore
  public boolean isFree() {
    return null == this.storagePoolId || this.storagePoolId.equals(PoolInfo.FREE_POOLID);
  }

  /**
   * reset archive free, it will be available to be added to storage pool.
   */
  @JsonIgnore
  public void setFree() {
    this.storagePoolId = PoolInfo.FREE_POOLID;
  }

  @JsonIgnore
  public int getTotalPageToMigrate() {
    return totalPageToMigrate;
  }

  @JsonIgnore

  public void setTotalPageToMigrate(int totalPageToMigrate) {
    this.totalPageToMigrate = totalPageToMigrate;
  }

  @JsonIgnore

  public void addTotalPageToMigrate(long totalPageToMigrate) {
    this.totalPageToMigrate += totalPageToMigrate;
  }

  @JsonIgnore

  public int getAlreadyMigratedPage() {
    return alreadyMigratedPage;
  }

  @JsonIgnore
  public void setAlreadyMigratedPage(int alreadyMigratedPage) {
    this.alreadyMigratedPage = alreadyMigratedPage;
  }

  @JsonIgnore
  public void addAlreadyMigratedPage(long alreadyMigratedPage) {
    this.alreadyMigratedPage += alreadyMigratedPage;
  }

  @JsonIgnore
  public int getMigrationSpeed() {
    return migrationSpeed;
  }

  @JsonIgnore
  public void setMigrationSpeed(int migrationSpeed) {
    this.migrationSpeed = migrationSpeed;
  }

  @JsonIgnore
  public void addMigrationSpeed(int migrationSpeed) {
    this.migrationSpeed += migrationSpeed;
  }

  @JsonIgnore
  public long getMaxMigrationSpeed() {
    return maxMigrationSpeed;
  }

  @JsonIgnore
  public void setMaxMigrationSpeed(long maxMigrationSpeed) {
    this.maxMigrationSpeed = maxMigrationSpeed;
  }

  @JsonIgnore
  public MigrationStrategy getMigrationStrategy() {
    return migrationStrategy;
  }

  @JsonIgnore
  public void setMigrationStrategy(MigrationStrategy migrationStrategy) {
    this.migrationStrategy = migrationStrategy;
  }

  @JsonIgnore
  public Long getUsedSpace() {
    return usedSpace;
  }

  @JsonIgnore
  public void setUsedSpace(Long usedSpace) {
    this.usedSpace = usedSpace;
  }

  public int getArbiterLimit() {
    return arbiterLimit;
  }

  public void setArbiterLimit(int arbiterLimit) {
    this.arbiterLimit = arbiterLimit;
  }

  public int getFlexiableLimit() {
    return flexiableLimit;
  }

  public void setFlexiableLimit(int flexiableLimit) {
    this.flexiableLimit = flexiableLimit;
  }

  public long getVirtualSegmentUnitDataOffset() {
    return virtualSegmentUnitDataOffset;
  }

  public void setVirtualSegmentUnitDataOffset(long virtualSegmentUnitDataOffset) {
    this.virtualSegmentUnitDataOffset = virtualSegmentUnitDataOffset;
  }

  public long getSegmentUnitMetadataOffset() {
    return segmentUnitMetadataOffset;
  }

  public void setSegmentUnitMetadataOffset(long segmentUnitMetadataOffset) {
    this.segmentUnitMetadataOffset = segmentUnitMetadataOffset;
  }

  public long getBrickMetadataOffset() {
    return brickMetadataOffset;
  }

  public void setBrickMetadataOffset(long brickMetadataOffset) {
    this.brickMetadataOffset = brickMetadataOffset;
  }
}
