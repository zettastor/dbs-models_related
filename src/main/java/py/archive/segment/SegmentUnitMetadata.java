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

package py.archive.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * One unit of the seg 's meta data. The max length of this class instance should not be larger than
 * 1024 bytes.
 *
 * <p>The reason to define the max length is to make it easier to store the metadata to a page
 */
public class SegmentUnitMetadata extends AbstractSegmentUnitMetadata implements Comparable<Object> {
  public static final BlockingQueue<SegmentUnitMetadata> justCreatedSegUnits
      = new LinkedBlockingQueue<>();
  @JsonIgnore
  public static final int MAX_LENGTH = 256; // bytes
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitMetadata.class);
  // The type of the volume this segment unit belongs to
  private final VolumeType volumeType;
  /**
   * mark: if status is DELETED been persisted into disk.
   */
  @JsonIgnore
  protected AtomicBoolean isPersistDeletedStatus = new AtomicBoolean(false);

  /**
   * Why we put the segment's membership to one of its units? When voting for a new primary,
   * everyone in the segment has to agree on the membership. Therefore, each segment unit should
   * know its segment's membership.
   *
   * <p>The second reason is that we want to persist the membership info to each node so that when
   * the node starts up, it can start to join the group instead of sending requests to the
   * membership service to get the membership. This is special useful (optimal) when the membership
   * service doesn't have very high availability
   *
   * <p>The reason that we make this variable volatile is that this variable, i.e., the membership
   * reference is changed frequently due to the expiration of alive secondaries. If the membership
   * is reassigned to a different one in one thread, it would be better for other threads should
   * know the change right away.
   */
  /* mutable fields */
  @JsonSerialize(using = JsonMembershipSerialize.class)
  @JsonDeserialize(using = JsonMembershipDeserialize.class)
  private volatile SegmentMembership membership;
  /**
   * there are a couple of threads that can change segment unit status. One is an activity thread
   * that receives a leader request to change status from "Start" to Joining, or from "Joining" to
   * Active. The second is PaxosProposer that can change status from 'Start' to "Leader Joining".
   * The last one is the leaderDriver that can change status from "Joining Leader" to leader.
   * Therefore, we need to synchronize the update of the status.
   *
   * <p>Also, we need to persist status because when a segment unit is deleted or deleting, we want
   * the unit to stay that way.
   */
  private SegmentUnitStatus status;
  private volatile int migrationStatus = MigrationStatus.NONE.getValue();

  /**
   * this field is used by data node. Data node uses it to track when the metadata was changed last
   * time. As information report to info center Most likely, status or membership were changed
   * because other fields are seldom changed. Data node persists lastUpdated while does not report
   * it to the info center.
   */
  private long lastUpdated;
  /**
   * this field is used by info center to record when segment_unit report. Info center uses it to
   * determine whether a unit is stale and change to "Unknown" status
   */
  private long lastReported;
  /**
   * offset where the nodes of page indexer are stored in disk.
   */
  private long pageIndexerOffset;
  /**
   * The following two fields are used to record the basic information of the account who created
   * this segment unit and its volume information. These information can be used to restore account
   * database and volume database if the database is corrupted or lost.
   */
  // a json string that can be parsed to AccountMetadata
  @JsonIgnore
  private String accountMetadataJson;
  // a json string that can be parsed to VolumeMetadata
  private String volumeMetadataJson;

  @JsonSerialize(using = JsonMembershipSerialize.class)
  @JsonDeserialize(using = JsonMembershipDeserialize.class)
  private volatile SegmentMembership srcMembership;
  private long srcVolumeId;

  // fields for rebalance-support
  private boolean isSecondaryCandidate;
  private InstanceId replacee;
  @JsonIgnore
  private double ratioMigration;
  // end for rebalance-support
  /**
   * the log id of last time bitmap persisted.
   */
  private long logIdOfPersistBitmap = 0;
  // disk name on which segment unit allocated; this field is not written to disk. For the disk name
  // may change when datanode start
  // when the segment unit read from disk, we just set the disk name;
  @JsonIgnore
  private String diskName;
  // archive id of the disk
  @JsonIgnore
  private long archiveId;
  @JsonIgnore
  private InstanceId instanceId;
  // page migration info report to infocenter
  @JsonIgnore
  private int totalPageToMigrate;
  @JsonIgnore
  private int alreadyMigratedPage;
  @JsonIgnore
  private int migrationSpeed;
  // page migration info receive from infocenter
  @JsonIgnore
  private long minMigrationSpeed;
  @JsonIgnore
  private long maxMigrationSpeed;
  @JsonIgnore
  private boolean innerMigrating;
  @JsonIgnore
  private boolean enableLaunchMultiDrivers;
  private VolumeMetadata.VolumeSourceType volumeSource;
  @JsonIgnore
  private InstanceId instanceIdOfRollBackJs;
  private AtomicBoolean missLogWhenPsi = new AtomicBoolean(false);
  private boolean newBorn = true;

  @JsonCreator
  public SegmentUnitMetadata(@JsonProperty("segId") SegId segId,
      @JsonProperty("offset") long offset) {
    super(segId, offset);
    this.volumeType = VolumeType.DEFAULT_VOLUME_TYPE;
  }

  public SegmentUnitMetadata(SegmentUnitMetadata metadata) {
    super(metadata.getSegId(), metadata.getMetadataOffsetInArchive(),
        metadata.getLogicalDataOffset(), metadata.getStorage());
    this.membership = new SegmentMembership(metadata.membership);
    this.lastUpdated = System.currentTimeMillis();
    this.status = metadata.status;
    this.diskName = metadata.diskName;
    this.archiveId = metadata.archiveId;
    this.volumeType = metadata.volumeType;
    this.accountMetadataJson = metadata.accountMetadataJson;
    this.volumeMetadataJson = metadata.volumeMetadataJson;
    this.segmentUnitType = metadata.segmentUnitType;
    this.bitmap = SegmentUnitBitmap.valueOf(metadata.getBitmap().toByteArray());
    this.migrationStatus = metadata.migrationStatus;
    this.ratioMigration = metadata.ratioMigration;
    this.isSecondaryCandidate = metadata.isSecondaryCandidate;
    this.replacee = metadata.replacee;
    this.missLogWhenPsi.set(metadata.missLogWhenPsi.get());
    if (metadata.instanceId != null) {
      this.instanceId = new InstanceId(metadata.instanceId);
    }
    this.maxMigrationSpeed = metadata.maxMigrationSpeed;
    this.minMigrationSpeed = metadata.minMigrationSpeed;
    this.migrationSpeed = metadata.migrationSpeed;
    this.totalPageToMigrate = metadata.totalPageToMigrate;
    this.alreadyMigratedPage = metadata.alreadyMigratedPage;
    this.enableLaunchMultiDrivers = metadata.enableLaunchMultiDrivers;
    this.volumeSource = metadata.volumeSource;
    this.srcVolumeId = metadata.getSrcVolumeId();
    if (metadata.getSrcMembership() != null) {
      this.srcMembership = new SegmentMembership(metadata.getSrcMembership());
    }
    this.logIdOfPersistBitmap = metadata.getLogIdOfPersistBitmap();
    super.setFreePageCount(metadata.getFreePageCount());
  }

  public SegmentUnitMetadata(SegId segId, long offset, SegmentMembership membership,
      SegmentUnitStatus status,
      VolumeType volumeType, SegmentUnitType type) {
    super(segId, offset);
    this.membership = membership;
    this.status = status;
    this.volumeType = volumeType;
    this.segmentUnitType = type;
    this.ratioMigration = 0;
  }

  public SegmentUnitMetadata(SegId segId, long dataOffset, SegmentMembership membership,
      SegmentUnitStatus status,
      long metadataOffset, Storage storage, VolumeType volumeType, SegmentUnitType type) {
    this(segId, dataOffset, membership, status, metadataOffset, storage, volumeType, type,
        null, null, 0L, null);

  }

  public SegmentUnitMetadata(SegId segId, long dataOffset, SegmentMembership membership,
      SegmentUnitStatus status,
      long metadataOffset, Storage storage, VolumeType volumeType, SegmentUnitType type,
      String volumeMetadataJson, String accountMetadataJson, long srcVolumeId,
      SegmentMembership srcMembership) {
    this(segId, dataOffset, membership, status, metadataOffset, storage, volumeType, type,
        volumeMetadataJson, accountMetadataJson, srcVolumeId, srcMembership,
        false, null);
  }

  public SegmentUnitMetadata(SegId segId, long dataOffset, SegmentMembership membership,
      SegmentUnitStatus status, long metadataOffset, Storage storage, VolumeType volumeType,
      SegmentUnitType type,
      String volumeMetadataJson, String accountMetadataJson, long srcVolumeId,
      SegmentMembership srcMembership, boolean isSecondaryCandidate, InstanceId replacee) {
    super(segId, metadataOffset, dataOffset, storage);
    this.membership = membership;
    this.lastUpdated = System.currentTimeMillis();
    this.status = status;
    if (volumeType == null) {
      this.volumeType = VolumeType.DEFAULT_VOLUME_TYPE;
    } else {
      this.volumeType = volumeType;
    }
    this.accountMetadataJson = accountMetadataJson;
    this.volumeMetadataJson = volumeMetadataJson;
    this.segmentUnitType = type;
    this.srcVolumeId = srcVolumeId;
    this.srcMembership = srcMembership;
    this.isSecondaryCandidate = isSecondaryCandidate;
    this.replacee = replacee;
  }

  public static <K, V> Map<V, K> reverse(Map<K, V> map) {
    HashMap<V, K> rev = new HashMap<V, K>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      rev.put(entry.getValue(), entry.getKey());
    }
    return rev;
  }

  @JsonIgnore
  public boolean isOnlineMigrationSegmentUnit() {
    return false;
  }

  public SegmentUnitStatus getStatus() {
    return status;
  }

  public void setStatus(SegmentUnitStatus status) {
    this.status = status;

    if (becomePrimaryRightAfterCreation()) {
      // I am just created
      doneWithCreation();
    }
  }

  public long getLastReported() {
    return lastReported;
  }

  public void setLastReported(long lastReported) {
    this.lastReported = lastReported;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public SegmentMembership getMembership() {
    return membership;
  }

  public void setMembership(SegmentMembership membership) {
    this.membership = membership;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(Long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public String getAccountMetadataJson() {
    return accountMetadataJson;
  }

  public void setAccountMetadataJson(String accountMetadataJson) {
    this.accountMetadataJson = accountMetadataJson;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public String getVolumeMetadataJson() {
    return volumeMetadataJson;
  }

  public void setVolumeMetadataJson(String volumeMetadataJson) {
    this.volumeMetadataJson = volumeMetadataJson;
  }

  public double getRatioMigration() {
    return ratioMigration;
  }

  public void setRatioMigration(double ratioMigration) {
    this.ratioMigration = ratioMigration;
  }

  @JsonIgnore
  @Override
  public int hashCode() {
    return Objects.hash(volumeType, membership, status, migrationStatus,
        lastUpdated, pageIndexerOffset, accountMetadataJson, volumeMetadataJson,
        srcMembership, srcVolumeId, isSecondaryCandidate, replacee, ratioMigration,
        logIdOfPersistBitmap,
        diskName, archiveId, instanceId,
        enableLaunchMultiDrivers, newBorn, volumeSource);
  }

  @JsonIgnore
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentUnitMetadata that = (SegmentUnitMetadata) o;
    return migrationStatus == that.migrationStatus
        && lastUpdated == that.lastUpdated
        && pageIndexerOffset == that.pageIndexerOffset
        && srcVolumeId == that.srcVolumeId
        && isSecondaryCandidate == that.isSecondaryCandidate
        && Double.compare(that.ratioMigration, ratioMigration) == 0
        && logIdOfPersistBitmap == that.logIdOfPersistBitmap && archiveId == that.archiveId
        && totalPageToMigrate == that.totalPageToMigrate
        && alreadyMigratedPage == that.alreadyMigratedPage
        && migrationSpeed == that.migrationSpeed && minMigrationSpeed == that.minMigrationSpeed
        && maxMigrationSpeed == that.maxMigrationSpeed && innerMigrating == that.innerMigrating
        && enableLaunchMultiDrivers == that.enableLaunchMultiDrivers && newBorn == that.newBorn
        && volumeType == that.volumeType
        && Objects
        .equals(membership, that.membership) && status == that.status && Objects
        .equals(accountMetadataJson, that.accountMetadataJson) && Objects
        .equals(volumeMetadataJson, that.volumeMetadataJson)
        && Objects.equals(srcMembership, that.srcMembership)
        && Objects.equals(replacee, that.replacee)
        && Objects.equals(diskName, that.diskName)
        && Objects.equals(instanceId, that.instanceId) && Objects
        .equals(volumeSource, that.volumeSource);
  }

  /**
   * Update the content from data node report segment unit.
   */
  @JsonIgnore
  public void updateWithNewOne(SegmentUnitMetadata newOne) {
    this.status = newOne.status;
    this.membership = new SegmentMembership(newOne.getMembership());
    this.diskName = newOne.diskName;
    this.archiveId = newOne.archiveId;
    this.migrationStatus = newOne.migrationStatus;
    this.ratioMigration = newOne.ratioMigration;
    this.migrationSpeed = newOne.migrationSpeed;
    this.totalPageToMigrate = newOne.totalPageToMigrate;
    this.alreadyMigratedPage = newOne.alreadyMigratedPage;
    this.minMigrationSpeed = newOne.minMigrationSpeed;
    this.maxMigrationSpeed = newOne.maxMigrationSpeed;
    this.srcVolumeId = newOne.srcVolumeId;
  }

  @JsonIgnore
  public boolean isSegmentUnitDeleted() {
    return status.equals(SegmentUnitStatus.Deleted);
  }

  @JsonIgnore
  public boolean isSegmentUnitMarkedAsDeleting() {
    return status.equals(SegmentUnitStatus.Deleting);
  }

  @JsonIgnore
  public boolean isSegmentUnitBroken() {
    return status.equals(SegmentUnitStatus.Broken);
  }

  @Override
  public String toString() {
    return "SegmentUnitMetadata{"
        + "volumeType=" + volumeType
        + ", isPersistDeletedStatus=" + isPersistDeletedStatus
        + ", membership=" + membership
        + ", status=" + status
        + ", migrationStatus=" + migrationStatus
        + ", lastUpdated=" + lastUpdated
        + ", lastReported=" + lastReported
        + ", pageIndexerOffset=" + pageIndexerOffset
        + ", accountMetadataJson='" + accountMetadataJson + '\''
        + ", volumeMetadataJson='" + volumeMetadataJson + '\''
        + ", srcMembership=" + srcMembership
        + ", srcVolumeId=" + srcVolumeId
        + ", isSecondaryCandidate=" + isSecondaryCandidate
        + ", replacee=" + replacee
        + ", ratioMigration=" + ratioMigration
        + ", logIdOfPersistBitmap=" + logIdOfPersistBitmap
        + ", diskName='" + diskName + '\''
        + ", archiveId=" + archiveId
        + ", instanceId=" + instanceId
        + ", enableLaunchMultiDrivers=" + enableLaunchMultiDrivers
        + ", volumeSource=" + volumeSource
        + ", instanceIdOfRollBackJs=" + instanceIdOfRollBackJs
        + ", missLogWhenPsi=" + missLogWhenPsi
        + ", newBorn=" + newBorn
        + '}';
  }

  @Override
  public int compareTo(Object o) {
    SegmentUnitMetadata other = (SegmentUnitMetadata) o;
    SegmentMembership ship1 = membership;
    SegmentMembership ship2 = other.getMembership();

    if (ship1 == null && ship2 == null) {
      return 0;
    }

    if (ship1 == null) {
      return -1;
    }

    if (ship2 == null) {
      return 1;
    }

    return ship1.compareVersion(ship2);
  }

  @JsonIgnore
  public String getDiskName() {
    return diskName;
  }

  @JsonIgnore
  public void setDiskName(String diskName) {
    this.diskName = diskName;
  }

  @JsonIgnore
  public long getArchiveId() {
    return archiveId;
  }

  @JsonIgnore
  public void setArchiveId(long archiveId) {
    this.archiveId = archiveId;
  }

  /**
   * a PreSecondary will be set MigratedStatus soon, so this method can be replaced by the next one
   * but actually there is a small window before MigratedStatus set.
   */
  @JsonIgnore
  public boolean isPreSecondaryStatus() {
    return SegmentUnitStatus.PreSecondary == this.status;
  }

  @JsonIgnore
  public boolean isMigratedStatus() {
    return this.getMigrationStatus().isMigratedStatus();
  }

  /**
   * Check if the segment unit becomes the primary right after it was created.
   */
  private boolean becomePrimaryRightAfterCreation() {
    return (newBorn && SegmentUnitStatus.Primary == this.status);
  }

  /**
   * Done with creation of this segment unit.
   */
  private boolean doneWithCreation() {
    justCreatedSegUnits.offer(this);
    this.newBorn = false;
    return !newBorn;
  }

  @Override
  public boolean isUnitReusable() {
    if (status != SegmentUnitStatus.Deleted) {
      return false;
    }

    if (isPersistDeletedStatus.get() == false) {
      logger.info("status is deleted, but no persist to disk.", this.toString());
      return false;
    }
    return true;
  }

  public SegmentMembership getSrcMembership() {
    return srcMembership;
  }

  public void setSrcMembership(SegmentMembership srcMembership) {
    this.srcMembership = srcMembership;
  }

  public long getSrcVolumeId() {
    return srcVolumeId;
  }

  public void setSrcVolumeId(long srcVolumeId) {
    this.srcVolumeId = srcVolumeId;
  }

  public boolean isSecondaryCandidate() {
    return isSecondaryCandidate;
  }

  public void setSecondaryCandidate(boolean secondaryCandidate) {
    isSecondaryCandidate = secondaryCandidate;
  }

  public InstanceId getReplacee() {
    return replacee;
  }

  public void setReplacee(InstanceId replacee) {
    this.replacee = replacee;
  }

  public long getLogIdOfPersistBitmap() {
    return logIdOfPersistBitmap;
  }

  public void setLogIdOfPersistBitmap(long logIdOfPersistBitmap) {
    this.logIdOfPersistBitmap = logIdOfPersistBitmap;
  }

  public long getPageIndexerOffset() {
    return pageIndexerOffset;
  }

  public void setPageIndexerOffset(long pageIndexerOffset) {
    this.pageIndexerOffset = pageIndexerOffset;
  }

  public MigrationStatus getMigrationStatus() {
    return MigrationStatus.findByValue(migrationStatus);
  }

  public void setMigrationStatus(MigrationStatus migrationStatus) {
    this.migrationStatus = migrationStatus.getValue();
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
  public int getAlreadyMigratedPage() {
    return alreadyMigratedPage;
  }

  @JsonIgnore
  public void setAlreadyMigratedPage(int alreadyMigratedPage) {
    this.alreadyMigratedPage = alreadyMigratedPage;
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
  public long getMinMigrationSpeed() {
    return minMigrationSpeed;
  }

  @JsonIgnore
  public void setMinMigrationSpeed(long minMigrationSpeed) {
    this.minMigrationSpeed = minMigrationSpeed;
  }

  @JsonIgnore
  public long getMaxMigrationSpeed() {
    return maxMigrationSpeed;
  }

  @JsonIgnore
  public void setMaxMigrationSpeed(long maxMigrationSpeed) {
    this.maxMigrationSpeed = maxMigrationSpeed;
  }

  public boolean isInnerMigrating() {
    return innerMigrating;
  }

  public void setInnerMigrating(boolean innerMigrating) {
    this.innerMigrating = innerMigrating;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public void setEnableLaunchMultiDrivers(boolean enableLaunchMultiDrivers) {
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  public VolumeMetadata.VolumeSourceType getVolumeSource() {
    return volumeSource;
  }

  public void setVolumeSource(VolumeMetadata.VolumeSourceType volumeSource) {
    this.volumeSource = volumeSource;
  }

  public AtomicBoolean getMissLogWhenPsi() {
    return missLogWhenPsi;
  }

  public void setMissLogWhenPsi(AtomicBoolean missLogWhenPsi) {
    this.missLogWhenPsi = missLogWhenPsi;
  }

  @JsonIgnore
  public AtomicBoolean getIsPersistDeletedStatus() {
    return isPersistDeletedStatus;
  }

  @JsonIgnore
  public void setIsPersistDeletedStatus(boolean status) {
    this.isPersistDeletedStatus.set(status);
  }

  @JsonIgnore
  public InstanceId getInstanceIdOfRollBackJs() {
    return instanceIdOfRollBackJs;
  }

  @JsonIgnore
  public void markJsisRollBacking(InstanceId instanceIdOfRollBackJs) {
    this.instanceIdOfRollBackJs = instanceIdOfRollBackJs;
  }

  @JsonIgnore
  public void clearRollBackingJs() {
    this.instanceIdOfRollBackJs = null;
  }
}