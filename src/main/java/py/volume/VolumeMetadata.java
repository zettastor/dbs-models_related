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

package py.volume;

import static py.volume.VolumeInAction.NULL;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.LimitQueue;
import py.infocenter.common.InfoCenterConstants;
import py.informationcenter.Utils;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.share.VolumeSourceThrift;

/**
 * This class store a volume information. Its object can be parsed to a json object and stored to
 * segment unit metadata.
 *
 * <p>Note: We need to move this class to DataNode package in future
 *
 * <p>In order to support the function of extending a volume, a volume can have a list of
 * child-volumes. It is as usual to create a root volume who does not have parents. In order to
 * extend a root volume, a new volume is created and its parentVolumeId is the id of the root
 * volume, and the root volume needs to include the newly created volume and increase its
 * totalVolumeSize by the size of the newly created volume. Note that a child-volume is not allowed
 * to have a child-volume currently.
 *
 * <p>we either want to make this class thread-safe or completely remove all concurrent maps in
 * the class and require whoever calls this class to take the responsibility of synchronizing the
 * class methods
 *
 */
public class VolumeMetadata implements Comparable<VolumeMetadata> {
  private static final Logger logger = LoggerFactory.getLogger(VolumeMetadata.class);
  @JsonIgnore
  private static final int DEFAULT_STORE_HISTORY_OF_SEGMENT_MEMBERSHIP = 3;
  /**
   * just for create volume time, mill second .
   */
  @JsonIgnore
  long waitToCreateUnitTime;
  // the id of parent volume. If a volume is the ancestor then its parent id
  // is 0.
  // Currently, only at most 2 levels are supported
  private long rootVolumeId;
  private long volumeId;
  private long volumeSize;
  // how big is the volume being extended. The extending size is not put to
  // the json string
  @JsonIgnore
  private long extendingSize;
  // all my child volume id. could be empty
  private Long childVolumeId;
  private String name;
  private long accountId;
  private int positionOfFirstSegmentInLogicVolume;
  // every time when childVolumeId, positionOfFirstSegmentInLogicVolume or
  // tagValue are changed,
  // version increments by 1. This value is used to decide which volume
  // meta data has most recent
  // value when persisting to segment unit meta data
  private int version;
  private Long domainId;
  private Long storagePoolId;
  private int segmentNumToCreateEachTime;
  @JsonIgnore
  private boolean updatedToDataNode;
  @JsonIgnore
  private long lastFixVolumeTime;
  @JsonIgnore
  private boolean persistedToDatabase;
  /**
   * The segment size in the volume. This provides flexible to make segment size difference from
   * other volumes.
   *
   * <p>However, once the volume is created, the segment size can't be changed.
   */
  @JsonIgnore
  private long segmentSize;

  // Map from the logic index of a segment to its metadata
  // @JsonIgnore
  // private Map<Integer, SegmentMetadata> segmentTableKeyedByLogicIndex;
  // Map from the logic index of a segment to its segment membership
  @JsonIgnore
  private Map<Integer, LimitQueue<SegmentMembership>> memberships;
  @JsonIgnore
  private Map<Integer, SegmentMetadata> segmentTable;
  @JsonIgnore
  private Map<Integer, SegmentMetadata> extendSegmentTable;
  /**
   * The range set records which segment has been asked to be created. If a volume is
   * simple-configured, we need to consult the volume layout when judging it available or not
   */
  @JsonIgnore
  private RangeSet<Integer> volumeLayoutRange;
  private String volumeLayoutString;
  @JsonIgnore
  private boolean needToPersistVolumeLayout = false;
  // What instances are involved
  @JsonIgnore
  private Set<InstanceId> instances;
  private VolumeType volumeType;
  @JsonIgnore
  private volatile VolumeStatus volumeStatus;
  @JsonIgnore
  private volatile VolumeExtendStatus volumeExtendStatus;
  private long deadTime;
  @JsonIgnore
  private double freeSpaceRatio = 1.0;
  @JsonIgnore
  private Map<Integer, SegId> mapLogicIndexToSegId;
  //add the date time, just for show in console
  private Date volumeCreatedTime;
  private Date lastExtendedTime;
  private VolumeSourceType volumeSource;
  private ReadWriteType readWrite;
  @JsonIgnore
  private VolumeInAction inAction = NULL;
  private int pageWrappCount;
  private int segmentWrappCount;
  private boolean enableLaunchMultiDrivers;
  @JsonIgnore
  private long totalPageToMigrate;
  @JsonIgnore
  private long alreadyMigratedPage;
  @JsonIgnore
  private long migrationSpeed;
  @JsonIgnore
  private double migrationRatio;
  @JsonIgnore
  private boolean leftSegment;
  @JsonIgnore
  private int nextStartSegmentIndex;
  /**
   * usually if you use this list to mark segment add order, please make sure enable add order when
   * you add each segment.
   */
  @JsonIgnore
  private List<Integer> addSegmentOrder;
  @JsonIgnore
  private long totalPhysicalSpace;
  //for make each time extend volume size,if not extend, set the root volume size
  private String eachTimeExtendVolumeSize;
  /**
   * about rebalance.
   */
  @JsonIgnore
  //rebalance infomation
  private VolumeRebalanceInfo rebalanceInfo = new VolumeRebalanceInfo();
  //volume stable time(ms, do not update when doing  rebalance util rebalance over)
  private long stableTime;

  /**
   * add this for link clone src volume delete process.
   */
  @JsonIgnore
  private boolean markDelete;

  @JsonIgnore
  private long clientLastConnectTime;

  private String volumeDescription;

  // Leave here for JSON serialization and deserialization
  public VolumeMetadata() {
    this(0, 0, 0, 0, null, 0, 0);
  }

  public VolumeMetadata(long rootVolumeId, long volumeId, long volumeSize, long segmentSize,
      VolumeType volumeType, long domainId, long storagePoolId) {
    this.rootVolumeId = rootVolumeId;
    this.volumeId = volumeId;
    this.volumeSize = volumeSize;
    this.segmentSize = segmentSize;
    this.volumeType = volumeType;
    this.volumeStatus = VolumeStatus.ToBeCreated;
    this.volumeExtendStatus = VolumeExtendStatus.ToBeCreated;

    segmentTable = new ConcurrentHashMap<>();
    extendSegmentTable = new ConcurrentHashMap<>();
    memberships = new ConcurrentHashMap<>();
    volumeLayoutRange = TreeRangeSet.create();

    childVolumeId = null;
    instances = new HashSet<>();
    positionOfFirstSegmentInLogicVolume = 0;
    updatedToDataNode = false;
    persistedToDatabase = false;
    version = 0;
    deadTime = 0;

    this.domainId = domainId;
    this.storagePoolId = storagePoolId;
    this.mapLogicIndexToSegId = new HashMap<>();
    this.addSegmentOrder = new ArrayList<>();
    //this.volumeSnapshotCapacity = new VolumeSnapshotCapacity(volumeId);
    this.markDelete = false;
    this.clientLastConnectTime = 0;
  }

  public static VolumeMetadata fromJson(String json)
      throws JsonParseException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, VolumeMetadata.class);
  }

  public boolean initVolumeLayout() {
    if (volumeSize == 0 || segmentSize == 0) {
      return false;
    } else {
      int segmentCount = getSegmentCount();

      volumeLayoutRange.add(Range.closed(0, segmentCount));
      updateVolumeLayoutString();
    }
    return true;
  }

  /**
   * if add the parameter in VolumeInformation class, must add the equals in this function just for
   * save the volume info to db.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VolumeMetadata that = (VolumeMetadata) o;
    return rootVolumeId == that.rootVolumeId
        && volumeId == that.volumeId
        && volumeSize == that.volumeSize
        && extendingSize == that.extendingSize
        && accountId == that.accountId
        && segmentNumToCreateEachTime == that.segmentNumToCreateEachTime
        && segmentSize == that.segmentSize
        && deadTime == that.deadTime
        && Double.compare(that.freeSpaceRatio, freeSpaceRatio) == 0
        && pageWrappCount == that.pageWrappCount
        && segmentWrappCount == that.segmentWrappCount
        && enableLaunchMultiDrivers == that.enableLaunchMultiDrivers
        && Objects.equals(name, that.name)
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(storagePoolId, that.storagePoolId)
        && Objects.equals(volumeLayoutString, that.volumeLayoutString)
        && volumeType == that.volumeType
        && volumeStatus == that.volumeStatus
        && Objects.equals(volumeCreatedTime, that.volumeCreatedTime)
        && Objects.equals(lastExtendedTime, that.lastExtendedTime)
        && volumeSource == that.volumeSource
        && readWrite == that.readWrite
        && Objects.equals(inAction, that.inAction)
        && Objects.equals(eachTimeExtendVolumeSize, that.eachTimeExtendVolumeSize)
        && rebalanceInfo.getRebalanceVersion() == that.rebalanceInfo.getRebalanceVersion()
        && volumeDescription == that.volumeDescription
        && markDelete == that.markDelete;
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(rootVolumeId, volumeId, volumeSize, extendingSize, childVolumeId, name, accountId,
            positionOfFirstSegmentInLogicVolume, version, domainId, storagePoolId,
            segmentNumToCreateEachTime, updatedToDataNode, lastFixVolumeTime,
            persistedToDatabase, segmentSize, volumeLayoutString, needToPersistVolumeLayout,
            volumeType,
            volumeStatus, deadTime, freeSpaceRatio, volumeCreatedTime, lastExtendedTime,
            volumeSource, readWrite,
            inAction, eachTimeExtendVolumeSize,
            markDelete, volumeDescription);
  }

  public long getClientLastConnectTime() {
    return clientLastConnectTime;
  }

  public void setClientLastConnectTime(long clientLastConnectTime) {
    this.clientLastConnectTime = clientLastConnectTime;
  }

  public String getVolumeDescription() {
    return volumeDescription;
  }

  public void setVolumeDescription(String volumeDescription) {
    this.volumeDescription = volumeDescription;
  }

  public VolumeMetadata deepCopy(VolumeMetadata src) {
    if (null == src) {
      return null;
    }
    this.rootVolumeId = src.getRootVolumeId();
    this.volumeId = src.getVolumeId();
    this.volumeSize = src.getVolumeSize();
    this.extendingSize = src.getExtendingSize();
    this.childVolumeId = src.getChildVolumeId();
    this.name = src.getName();
    this.accountId = src.getAccountId();
    this.positionOfFirstSegmentInLogicVolume = src.getPositionOfFirstSegmentInLogicVolume();
    this.version = src.getVersion();
    this.domainId = src.getDomainId();
    this.storagePoolId = src.getStoragePoolId();
    this.segmentNumToCreateEachTime = src.getSegmentNumToCreateEachTime();
    this.updatedToDataNode = src.isUpdatedToDataNode();
    this.lastFixVolumeTime = src.getLastFixVolumeTime();
    this.persistedToDatabase = src.isPersistedToDatabase();
    this.segmentSize = src.getSegmentSize();
    this.memberships = src.getMemberships();
    this.segmentTable = src.getSegmentTable();
    this.extendSegmentTable = src.getExtendSegmentTable();
    this.volumeLayoutRange = src.getVolumeLayoutRange();
    this.volumeLayoutString = src.getVolumeLayout();
    this.needToPersistVolumeLayout = src.isNeedToPersistVolumeLayout();
    this.instances = src.getInstances();
    this.volumeType = src.getVolumeType();
    this.volumeStatus = src.getVolumeStatus();
    this.volumeExtendStatus = src.getVolumeExtendStatus();
    this.deadTime = src.getDeadTime();
    this.freeSpaceRatio = src.getFreeSpaceRatio();
    this.mapLogicIndexToSegId = src.getMapLogicIndexToSegId();
    this.volumeCreatedTime = src.getVolumeCreatedTime();
    this.lastExtendedTime = src.getLastExtendedTime();
    this.volumeSource = src.getVolumeSource();
    this.readWrite = src.getReadWrite();
    this.inAction = src.getInAction();
    this.pageWrappCount = src.getPageWrappCount();
    this.segmentWrappCount = src.getSegmentWrappCount();
    this.enableLaunchMultiDrivers = src.isEnableLaunchMultiDrivers();
    this.rebalanceInfo.deepCopy(src.getRebalanceInfo());
    this.stableTime = src.getStableTime();
    this.totalPhysicalSpace = src.getTotalPhysicalSpace();
    this.waitToCreateUnitTime = src.getWaitToCreateUnitTime();
    this.eachTimeExtendVolumeSize = src.eachTimeExtendVolumeSize;
    this.markDelete = src.markDelete;
    this.volumeDescription = src.volumeDescription;
    this.clientLastConnectTime = src.clientLastConnectTime;
    this.migrationRatio = src.migrationRatio;
    this.totalPageToMigrate = src.totalPageToMigrate;

    return this;
  }

  public VolumeMetadata deepCopyNotIncludeSegment(VolumeMetadata src) {
    if (null == src) {
      return null;
    }
    this.rootVolumeId = src.getRootVolumeId();
    this.volumeId = src.getVolumeId();
    this.volumeSize = src.getVolumeSize();
    this.extendingSize = src.getExtendingSize();
    this.childVolumeId = src.getChildVolumeId();
    this.name = src.getName();
    this.accountId = src.getAccountId();
    this.positionOfFirstSegmentInLogicVolume = src.getPositionOfFirstSegmentInLogicVolume();
    this.version = src.getVersion();
    this.domainId = src.getDomainId();
    this.storagePoolId = src.getStoragePoolId();
    this.segmentNumToCreateEachTime = src.getSegmentNumToCreateEachTime();
    this.updatedToDataNode = src.isUpdatedToDataNode();
    this.lastFixVolumeTime = src.getLastFixVolumeTime();
    this.persistedToDatabase = src.isPersistedToDatabase();
    this.segmentSize = src.getSegmentSize();
    this.extendSegmentTable = src.getExtendSegmentTable();
    this.volumeLayoutRange = src.getVolumeLayoutRange();
    this.volumeLayoutString = src.getVolumeLayout();
    this.needToPersistVolumeLayout = src.isNeedToPersistVolumeLayout();
    this.instances = src.getInstances();
    this.volumeType = src.getVolumeType();
    this.volumeStatus = src.getVolumeStatus();
    this.volumeExtendStatus = src.getVolumeExtendStatus();
    this.deadTime = src.getDeadTime();
    this.freeSpaceRatio = src.getFreeSpaceRatio();
    this.mapLogicIndexToSegId = src.getMapLogicIndexToSegId();
    this.volumeCreatedTime = src.getVolumeCreatedTime();
    this.lastExtendedTime = src.getLastExtendedTime();
    this.volumeSource = src.getVolumeSource();
    this.readWrite = src.getReadWrite();
    this.inAction = src.getInAction();
    this.pageWrappCount = src.getPageWrappCount();
    this.segmentWrappCount = src.getSegmentWrappCount();
    this.enableLaunchMultiDrivers = src.isEnableLaunchMultiDrivers();
    this.rebalanceInfo.deepCopy(src.getRebalanceInfo());
    this.stableTime = src.getStableTime();
    this.totalPhysicalSpace = src.getTotalPhysicalSpace();
    this.waitToCreateUnitTime = src.getWaitToCreateUnitTime();
    this.eachTimeExtendVolumeSize = src.eachTimeExtendVolumeSize;
    this.markDelete = src.markDelete;
    this.volumeDescription = src.volumeDescription;
    this.clientLastConnectTime = src.clientLastConnectTime;

    return this;
  }

  /**
   * when we create volume and there is no segment created in this volume, we should delete volume
   * after timeout.
   */
  @JsonIgnore
  public boolean isVolumeCreateTimeout() {
    logger.debug("get the each time :{}, {}, {}, ", new Date(System.currentTimeMillis()),
        waitToCreateUnitTime, volumeCreatedTime);
    //check time out, must case the wait time which to create volume
    if (System.currentTimeMillis() - waitToCreateUnitTime - volumeCreatedTime.getTime()
        > TimeUnit.SECONDS.toMillis(InfoCenterConstants.getVolumeToBeCreatedTimeout())) {
      logger.warn(
          "when toBeCreate the volume :{} {}, the toBeCreate timeout :{}, the wait time is :{}",
          volumeId, name, volumeCreatedTime.getTime(), waitToCreateUnitTime);
      return true;
    } else {
      return false;
    }
  }

  @JsonIgnore
  public boolean isVolumeCreatingTimeout() {
    logger.debug("get the each time :{}, {}, {}, ", new Date(System.currentTimeMillis()),
        waitToCreateUnitTime, volumeCreatedTime);
    if (System.currentTimeMillis() - waitToCreateUnitTime - volumeCreatedTime.getTime()
        > TimeUnit.SECONDS
        .toMillis(InfoCenterConstants.getVolumeBeCreatingTimeout())) {
      logger.warn("when Creating the volume :{} {}, the Creating timeout :{}, the wait time is :{}",
          volumeId, name, volumeCreatedTime.getTime(), waitToCreateUnitTime);
      return true;
    } else {
      return false;
    }
  }

  @JsonIgnore
  public boolean isFixVolumeTimeout() {
    logger.warn("Fix Volume is Volume Time OUT,{}-{}>{}", System.currentTimeMillis(),
        lastFixVolumeTime,
        TimeUnit.SECONDS.toMillis(InfoCenterConstants.getFixVolumeTimeoutSec()));
    if (System.currentTimeMillis() - lastFixVolumeTime > TimeUnit.SECONDS
        .toMillis(InfoCenterConstants.getFixVolumeTimeoutSec())) {
      return true;
    } else {
      return false;
    }
  }

  @JsonIgnore
  public boolean isVolumeExtendTimeout() {
    logger.debug("get the each time :{}, {}, {}, ", new Date(System.currentTimeMillis()),
        waitToCreateUnitTime, volumeCreatedTime);
    if (lastExtendedTime != null
        && System.currentTimeMillis() - waitToCreateUnitTime - lastExtendedTime.getTime()
        > TimeUnit.SECONDS
        .toMillis(InfoCenterConstants.getVolumeBeCreatingTimeout())) {
      logger.warn(
          "when extend the volume :{} {}, the extend segment timeout :{}, the wait time is :{}",
          volumeId, name, lastExtendedTime.getTime(), waitToCreateUnitTime);
      return true;
    } else {
      return false;
    }
  }

  /**
   * This function iterates through all segments in this volume to check the volume status.
   */
  public synchronized VolumeStatus updateStatus() {
    VolumeStatus oldStatus = this.volumeStatus;
    VolumeStatus newStatus = oldStatus.getNextVolumeStatus(this);
    logger.info("volume:{} :{} old status:{}, new status:{}, and segment size :{}", volumeId, name,
        oldStatus, newStatus, segmentTable.size());
    while (newStatus != oldStatus) {
      logger.warn("volume status is change from {} to {}, volume:{} {} and segment size :{}",
          oldStatus, newStatus, volumeId, name, segmentTable.size());
      this.setVolumeStatus(newStatus);
      oldStatus = newStatus;
      newStatus = newStatus.getNextVolumeStatus(this);
    }
    return this.volumeStatus;
  }

  /**
   * This function iterates through all segments in this volume to check the volume status.
   */
  public synchronized VolumeExtendStatus updateExtendStatus() {
    VolumeExtendStatus oldExtendStatus = this.volumeExtendStatus;
    VolumeExtendStatus newExtendStatus = oldExtendStatus.getNextVolumeExtendStatus(this);
    logger.warn(
        "volume extend :{} {} old extend status:{}, new extend status:{}, segment size and "
            + "extendSegment size {} {}",
        volumeId, name, oldExtendStatus, newExtendStatus, segmentTable.size(),
        extendSegmentTable.size());
    while (newExtendStatus != oldExtendStatus) {
      logger.warn(
          "volume extend status is change from {} to {}, volume:{} {} segment size and "
              + "extendSegment size {} {}",
          oldExtendStatus, newExtendStatus, volumeId, name, segmentTable.size(),
          extendSegmentTable.size());
      this.setVolumeExtendStatus(newExtendStatus);
      oldExtendStatus = newExtendStatus;
      newExtendStatus = newExtendStatus.getNextVolumeExtendStatus(this);
    }

    return this.volumeExtendStatus;
  }

  public synchronized VolumeInAction updateAction() {
    VolumeInAction oldAction = this.inAction;
    VolumeInAction newAction = oldAction.getNextVolumeAction(this);
    logger.info(
        "for updateAction, the volume:{} {} old Action:{}, new Action:{}, and volume source and "
            + "status:{} {}",
        volumeId, name, oldAction, newAction, this.getVolumeSource(), this.volumeStatus);
    while (newAction != oldAction) {
      logger.warn(
          "volume action is change from {} to {}, volume:{} {}, and volume source and status:{} {}",
          oldAction, newAction, volumeId, name, this.getVolumeSource(), this.volumeStatus);
      this.setInAction(newAction);
      oldAction = newAction;
      newAction = newAction.getNextVolumeAction(this);
    }
    return this.inAction;
  }

  public void updateMembership(int segIndex, SegmentMembership membership) {
    LimitQueue<SegmentMembership> segmentMemberships = memberships.get(segIndex);
    if (segmentMemberships == null) {
      String errMsg = "segment " + segIndex + " doesn't exist in the membership table";
      logger.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    segmentMemberships.offer(membership);
  }

  /**
   * might return null if no segment unit exist in this segment.
   */
  @JsonIgnore
  public SegmentMembership getMembership(int segIndex) {
    LimitQueue<SegmentMembership> segmentMembershipLimitQueue = memberships.get(segIndex);
    if (segmentMembershipLimitQueue == null) {
      return null;
    }
    return segmentMembershipLimitQueue.getLast();
  }

  public void addSegmentMetadata(SegmentMetadata segmentMetadata,
      SegmentMembership highestMembershipInSegment) {
    addSegmentMetadata(segmentMetadata, highestMembershipInSegment, false);
  }

  public void addSegmentMetadata(SegmentMetadata segmentMetadata,
      SegmentMembership highestMembershipInSegment,
      boolean enableAddSegmentOrder) {
    int segIndex = segmentMetadata.getIndex();
    if (enableAddSegmentOrder) {
      addSegmentOrder.add(segIndex);
    }
    segmentTable.put(segIndex, segmentMetadata);
    LimitQueue<SegmentMembership> segmentMemberships = memberships.get(segIndex);
    if (segmentMemberships == null) {
      segmentMemberships = new LimitQueue(DEFAULT_STORE_HISTORY_OF_SEGMENT_MEMBERSHIP);
      memberships.put(segIndex, segmentMemberships);
    }
    segmentMemberships.offer(highestMembershipInSegment);
    /*
     * segment also can track the volume which belong to, segment use it to get quorum when decide
     * segment status
     */
    segmentMetadata.setVolume(this);
  }

  public void addExtendSegmentMetadata(SegmentMetadata segmentMetadata,
      SegmentMembership highestMembershipInSegment) {
    addExtendSegmentMetadata(segmentMetadata, highestMembershipInSegment, false);
  }

  public void addExtendSegmentMetadata(SegmentMetadata segmentMetadata,
      SegmentMembership highestMembershipInSegment,
      boolean enableAddSegmentOrder) {
    int segIndex = segmentMetadata.getIndex();
    if (enableAddSegmentOrder) {
      addSegmentOrder.add(segIndex);
    }
    extendSegmentTable.put(segIndex, segmentMetadata);
    LimitQueue<SegmentMembership> segmentMemberships = memberships.get(segIndex);
    if (segmentMemberships == null) {
      segmentMemberships = new LimitQueue(DEFAULT_STORE_HISTORY_OF_SEGMENT_MEMBERSHIP);
      memberships.put(segIndex, segmentMemberships);
    }
    segmentMemberships.offer(highestMembershipInSegment);
    /*
     * segment also can track the volume which belong to, segment use it to get quorum when decide
     * segment status
     */
    segmentMetadata.setVolume(this);
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getRootVolumeId() {
    return rootVolumeId;
  }

  public void setRootVolumeId(long rootVolumeId) {
    this.rootVolumeId = rootVolumeId;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  /**
   * Get segment meta data by index.
   */
  @JsonIgnore
  public SegmentMetadata getSegmentByIndex(int index) {
    return segmentTable.get(index);
  }

  @JsonIgnore
  public SegmentMetadata getExtendSegmentByIndex(int index) {
    return extendSegmentTable.get(index);
  }

  @JsonIgnore
  public int getSegmentTableSize() {
    return segmentTable.size();
  }

  @JsonIgnore
  public int getSegmentCount() {
    return (int) (Math.ceil(volumeSize * 1.0 / segmentSize));
  }

  @JsonIgnore
  public int getExtendSegmentTableSize() {
    return extendSegmentTable.size();
  }

  @JsonIgnore
  public int getExtendSegmentCount() {
    return (int) (Math.ceil(extendingSize * 1.0 / segmentSize));
  }

  @JsonIgnore
  public int getRealSegmentCount() {
    return segmentTable.size();
  }

  /**
   * just for deleting volume.
   ***/
  @JsonIgnore
  public boolean getSomeSegmentHaveUnitOrNot() {
    boolean canToDead = false;
    for (SegmentMetadata segmentMetadata : segmentTable.values()) {
      if (segmentMetadata.checkCurrentSegmentIsEnpty()) {
        logger.warn("when delete volume:{}, find the segment :{} is empty", this.volumeId,
            segmentMetadata.getSegId().getIndex());
        canToDead = true;
      }
    }

    return canToDead;
  }

  /**
   * This function returns the list of segments in the volume. Note that there is no guarantee that
   * the segment at the front of the list has smaller index than those at the end.
   */
  @JsonIgnore
  public List<SegmentMetadata> getSegments() {
    List<SegmentMetadata> segmentsMetadata = new ArrayList<>();

    for (Map.Entry<Integer, SegmentMetadata> segmentMetadataEntry : segmentTable.entrySet()) {
      segmentsMetadata.add(segmentMetadataEntry.getValue());
    }
    return segmentsMetadata;
  }

  // This function is only for testing. In order to add a membership, we have
  // to go through addSegment()
  @JsonIgnore
  public Map<Integer, LimitQueue<SegmentMembership>> getMemberships() {
    return memberships;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public void setVolumeType(VolumeType volumeType) {
    this.volumeType = volumeType;
  }

  public Set<InstanceId> getInstances() {
    return instances;
  }

  public void setInstances(Set<InstanceId> instances) {
    this.instances = instances;
  }

  public void addInstance(InstanceId instanceId) {
    this.instances.add(instanceId);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public VolumeStatus getVolumeStatus() {
    return volumeStatus;
  }

  public void setVolumeStatus(VolumeStatus volumeStatus) {
    this.volumeStatus = volumeStatus;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public Long getChildVolumeId() {
    return childVolumeId;
  }

  public void setChildVolumeId(Long childVolumeId) {
    this.childVolumeId = childVolumeId;
  }

  public long getExtendingSize() {
    return extendingSize;
  }

  public void setExtendingSize(long extendingSize) {
    this.extendingSize = extendingSize;
  }

  public int getPositionOfFirstSegmentInLogicVolume() {
    return positionOfFirstSegmentInLogicVolume;
  }

  public void setPositionOfFirstSegmentInLogicVolume(int positionOfFirstSegmentInLogicVolume) {
    this.positionOfFirstSegmentInLogicVolume = positionOfFirstSegmentInLogicVolume;
  }

  public boolean isUpdatedToDataNode() {
    return updatedToDataNode;
  }

  public void setUpdatedToDataNode(boolean updatedToDataNode) {
    this.updatedToDataNode = updatedToDataNode;
  }

  public boolean isPersistedToDatabase() {
    return persistedToDatabase;
  }

  public void setPersistedToDatabase(boolean persistedToDatabase) {
    this.persistedToDatabase = persistedToDatabase;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getTotalPhysicalSpace() {
    return totalPhysicalSpace;
  }

  public void setTotalPhysicalSpace(long totalPhysicalSpace) {
    this.totalPhysicalSpace = totalPhysicalSpace;
  }

  public String toJsonString() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }

  @Override
  public String toString() {
    StringBuilder volumeInfo = new StringBuilder();
    volumeInfo.append("VolumeMetadata basic info as follow: \n[rootVolumeId=").append(rootVolumeId)
        .append(", volumeId=").append(volumeId).append(", volumeStatus=").append(volumeStatus)
        .append(", volumeSize=").append(volumeSize).append(", extendingSize=").append(extendingSize)
        .append(", childVolumeId=").append(childVolumeId).append(", name=").append(name)
        .append(", accountId=")
        .append(accountId).append("\n positionOfFirstSegmentInLogicVolume=")
        .append(positionOfFirstSegmentInLogicVolume).append(", version=").append(version)
        .append(", updatedToDataNode=").append(updatedToDataNode).append(", persistedToDatabase=")
        .append(persistedToDatabase).append(", segmentSize=").append(segmentSize)
        .append(", volumeType=")
        .append(volumeType).append(", tagKey=")
        .append(", volumeLayout=").append(volumeLayoutRange).append(", segmentNumToCreateEachTime=")
        .append(segmentNumToCreateEachTime)
        .append(", freeSpaceRatio=")
        .append(freeSpaceRatio)
        .append(", volumeCreatedTime=").append(volumeCreatedTime).append(", lastExtendedTime=")
        .append(lastExtendedTime).append(", volumeSource=").append(volumeSource)
        .append(", readWriteType=")
        .append(readWrite).append(", inAction=").append(inAction).append(", pageWrappCount=")
        .append(pageWrappCount).append(", segmentWrappCount=").append(segmentWrappCount)
        .append(", enableLaunchMultiDrivers=").append(enableLaunchMultiDrivers)
        .append(", eachTimeExtendVolumeSize=")
        .append(eachTimeExtendVolumeSize).append(", rebalanceRatio=")
        .append(rebalanceInfo.getRebalanceRatio()).append(", rebalanceVersion=")
        .append(rebalanceInfo.getRebalanceVersion()).append(", totalPhysicalSpace=")
        .append(totalPhysicalSpace)
        .append(", stableTime=").append(stableTime).append(", markDelete=").append(markDelete)
        .append(", volumeDescription=").append(volumeDescription)
        .append(", clientLastConnectTime=").append(clientLastConnectTime)
        .append("]");
    if (deadTime != 0) {
      volumeInfo.append(", deadTime=").append(Utils.millsecondToString(deadTime));
    }
    volumeInfo.append("]\n");

    if (null == memberships || 0 == memberships.size()) {
      volumeInfo.append("no membership in volume!!\n");
    } else {
      volumeInfo.append("membership info as follow:\n");
      for (Entry<Integer, LimitQueue<SegmentMembership>> entry : memberships.entrySet()) {
        volumeInfo.append("[index=").append(entry.getKey().toString()).append(",")
            .append(entry.getValue().toString()).append("]\n");
      }
    }

    if (null == segmentTable || 0 == segmentTable.size()) {
      volumeInfo.append("segment table has no content!!\n");
    } else {
      volumeInfo.append("segment table as follow:\n");
      for (Entry<Integer, SegmentMetadata> entry : segmentTable.entrySet()) {
        volumeInfo.append(entry.getValue().toString());
      }
    }

    return volumeInfo.toString();
  }

  @JsonIgnore
  public void incVersion() {
    version++;
  }

  @JsonIgnore
  public void decVersion() {
    version--;
  }

  @Override
  @JsonIgnore
  public int compareTo(VolumeMetadata o) {
    if (o == null) {
      return 1;
    } else {
      return this.version - o.version;
    }
  }

  @JsonIgnore
  public long getDeadTime() {
    return deadTime;
  }

  @JsonIgnore
  public void setDeadTime(long deadTime) {
    this.deadTime = deadTime;
  }

  @JsonIgnore
  public boolean isAllSegmentsAvailable() {
    logger.info("volume layout, {}", volumeLayoutString);
    int expectSegmentCount = getSegmentCount();

    boolean available = true;

    // if not all the segment has been reported from data node, return false
    logger.info("expect segment count:{}, current segment count:{}", expectSegmentCount,
        segmentTable.size());
    if (segmentTable.size() != expectSegmentCount) {
      logger.warn(
          "for volume :{},{} expect segment count:{}, current segment count:{}, the different",
          volumeId, name,
          expectSegmentCount, segmentTable.size());
      return false;
    }

    for (SegmentMetadata segmentMetadata : segmentTable.values()) {
      if (!segmentMetadata.getSegmentStatus().available()) {
        logger.warn("for volume :{}, {}, find the unavailable segment:{}", volumeId, name,
            segmentMetadata);
        available = false;
        break;
      }
    }
    return available;
  }

  @JsonIgnore
  public boolean isAllExtendSegmentsAvailable() {
    logger.info("volume layout, {}", volumeLayoutString);
    int extendSegmentCount = getExtendSegmentCount();

    boolean available = true;

    // if not all the segment has been reported from data node, return false
    logger.info("expect segment count:{}, current segment count:{}", extendSegmentCount,
        extendSegmentTable.size());
    if (extendSegmentTable.size() != extendSegmentCount) {
      logger.warn(
          "in extend volume :{}, {} expect segment count:{}, current segment count:{}, the"
              + " different",
          volumeId, name,
          extendSegmentCount, extendSegmentTable.size());
      return false;
    }

    for (SegmentMetadata segmentMetadata : extendSegmentTable.values()) {
      if (!segmentMetadata.getSegmentStatus().available()) {
        logger.warn("in extend volume :{}, {}, find the unavailable segment:{}", volumeId, name,
            segmentMetadata);
        available = false;
        break;
      }
    }
    return available;
  }

  @JsonIgnore
  public boolean isStable() {
    logger.info("to get the volume status isStable, the volume is :{}", volumeId);
    if (volumeStatus != VolumeStatus.Available && volumeStatus != VolumeStatus.Stable) {
      logger.warn("Not stable, volume is not available but {}", volumeStatus);
      return false;
    }

    List<SegmentMetadata> segDatas = getSegments();
    if (null == segDatas) {
      logger.error("segments is null, {} ", this);
      return false;
    }
    if (volumeStatus == VolumeStatus.Available || volumeStatus == VolumeStatus.Stable) {
      if (segDatas.isEmpty()) {
        logger.warn("Not stable : volume status is OK, but segments size is zero {}", this);
      }
    }

    // check if segment count equal to volume size divided by segment size
    if (segDatas.size() != volumeSize / segmentSize) {
      logger.warn("Not stable : volume doesn't have enough segments yet");
      return false;
    }

    for (SegmentMetadata segmentMetadata : segDatas) {
      SegmentMembership highestMembership = getHighestMembership(segmentMetadata);
      if (highestMembership == null || highestMembership.size() < 3) {
        logger.warn("Not stable, membership is null or size less than 3. SegId:{}",
            segmentMetadata.getSegId());
        return false;
      }

      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
          .getSegmentUnitMetadata(highestMembership.getPrimary());
      if (segmentUnitMetadata == null) {
        logger.warn("Not stable, segment unit meta data is null. SegId:{}",
            segmentMetadata.getSegId());
        return false;
      }

      // check that primary is ok
      if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Primary) {
        logger.warn(
            "Not stable, the primary we got from membership is not in status primary but {}. "
                + "SegId:{}",
            segmentUnitMetadata.getStatus(), segmentMetadata.getSegId());
        return false;
      }

      if (!highestMembership.getJoiningSecondaries().isEmpty()) {
        logger.warn("Not stable, there are still joining secondaries. SegId:{}",
            segmentMetadata.getSegId());
        return false;
      }

      if (!highestMembership.getInactiveSecondaries().isEmpty()) {
        logger.warn("Not stable, there are still inactive secondaries. SegId:{}",
            segmentMetadata.getSegId());
        return false;
      }

      for (InstanceId instanceId : highestMembership.getAllSecondaries()) {
        segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
        if (segmentUnitMetadata == null) {
          logger.warn("Not stable, a secondary we got from membership is not present. SegId:{}",
              segmentMetadata.getSegId());
          return false;
        } else if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Secondary
            && segmentUnitMetadata.getStatus() != SegmentUnitStatus.Arbiter) {
          logger.warn(
              "Not stable, a secondary we got from membership is not secondary or arbiter but {}. "
                  + "SegId:{}",
              segmentUnitMetadata.getStatus(), segmentMetadata.getSegId());
          return false;
        }
      }

      if (!segmentMetadata.getSegmentStatus().isStable()) {
        logger.warn("Not stable, segment's is not stable. SegId:{}",
            segmentMetadata.getSegId());
        return false;
      }

      // check for segment unit has the full membership
      for (SegmentUnitMetadata segUnit : segmentMetadata.getSegmentUnits()) {
        if (segUnit.getMembership().size() != volumeType.getNumMembers()) {
          logger.warn("Not stable, segment unit membership not full. SegId:{}",
              segmentMetadata.getSegId());
          return false;
        }
      }
    }

    logger.info("Volume {} is stable now", name);
    return true;
  }

  @JsonIgnore
  private SegmentMembership getHighestMembership(SegmentMetadata segmentMetadata) {
    SegmentMembership highestMembership = null;
    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segmentMetadata
        .getSegmentUnitMetadataTable()
        .entrySet()) {
      if (entry.getValue().getMembership().compareTo(highestMembership) > 0) {
        highestMembership = entry.getValue().getMembership();
      }
    }

    return highestMembership;
  }

  @JsonIgnore
  public boolean isAllSegmentInDeleting() {
    int segmentCount = (int) (this.getVolumeSize() / this.getSegmentSize());
    if (this.getSegmentTableSize() != segmentCount) {
      return false;
    }

    for (SegmentMetadata segmentMetadata : segmentTable.values()) {
      if (segmentMetadata.getSegmentStatus() != SegmentMetadata.SegmentStatus.Deleting) {
        return false;
      }
    }

    return true;
  }

  @JsonIgnore
  public boolean isSomeSegmentInDead() {
    boolean isDead = false;
    for (SegmentMetadata segmentMetadata : segmentTable.values()) {
      if (segmentMetadata.getSegmentStatus() == SegmentMetadata.SegmentStatus.Dead) {
        isDead = true;
        break;
      }
    }

    return isDead;
  }

  /**
   * check volume has been in deleting, deleted or dead status.
   */
  @JsonIgnore
  public boolean isDeletedByUser() {
    return (this.volumeStatus == VolumeStatus.Deleting || this.volumeStatus == VolumeStatus.Deleted
        || this.volumeStatus == VolumeStatus.Dead);
  }

  /**
   * check if volume can be recycled: two conditions must be satisfied: 1. volume status is in
   * Deleted status; 2. all the segment unit must have been clone done: the clone for volume may be
   * failed, if failed the volume will be in deleted status, it is meaningless to recycle a volume
   * that clone failed.
   */
  @JsonIgnore
  public boolean canBeRecycled() {
    return (this.volumeStatus == VolumeStatus.Deleted);
  }

  /**
   * Check volume is recovering, sometime user may choose to get the deleting volume back. This
   * volume can be in such status.
   */
  @JsonIgnore
  public boolean isRecycling() {
    return this.volumeStatus == VolumeStatus.Recycling;
  }

  public Long getDomainId() {
    return domainId;
  }

  public void setDomainId(Long domainId) {
    this.domainId = domainId;
  }

  public Long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(Long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public double getFreeSpaceRatio() {
    return freeSpaceRatio;
  }

  public void setFreeSpaceRatio(double freeSpaceRatio) {
    this.freeSpaceRatio = freeSpaceRatio;
  }

  public Date getVolumeCreatedTime() {
    return volumeCreatedTime;
  }

  public void setVolumeCreatedTime(Date volumeCreatedTime) {
    this.volumeCreatedTime = volumeCreatedTime;
  }

  public Date getLastExtendedTime() {
    return lastExtendedTime;
  }

  public void setLastExtendedTime(Date lastExtendedTime) {
    this.lastExtendedTime = lastExtendedTime;
  }

  public VolumeSourceType getVolumeSource() {
    return volumeSource;
  }

  public void setVolumeSource(VolumeSourceType volumeSource) {
    this.volumeSource = volumeSource;
  }

  // judge the volume whether is orphan volume

  @JsonIgnore
  public boolean isOrphanVolume() {
    if (this.getSegmentCount() == 0) {
      return false;
    }

    for (SegmentMetadata segment : this.getSegments()) {
      if (segment.getSegmentUnitCount() > 1) {
        return false;
      }
    }

    return true;
  }

  @JsonIgnore
  public void updateSegmentTableWhenExtendOk() {
    logger.warn("when extend the volume :{} ok, the extendSegmentTable size :{}", volumeId,
        extendSegmentTable.size());
    for (Map.Entry<Integer, SegmentMetadata> entry : extendSegmentTable.entrySet()) {
      SegmentMetadata segToBeAdded = entry.getValue();
      SegmentMembership latestMembershipInSegment = segToBeAdded.getLatestMembership();
      addSegmentMetadata(segToBeAdded, latestMembershipInSegment, true);
    }
    extendSegmentTable.clear();
  }

  @JsonIgnore
  public void clearTheExtendSegmentTable(int extendCount) {
    logger.warn(
        "when extend the volume :{} failed, the extendSegmentTable size :{}, remove :{} size info"
            + " in memberships",
        volumeId, extendSegmentTable.size(), extendCount);
    extendSegmentTable.clear();

    //the memberships remove
    int count = getSegmentCount();

    for (int i = 0; i < extendCount; i++) {
      memberships.remove(count + i);
      addSegmentOrder.remove(new Integer(count + i));
    }
  }

  public int getSegmentNumToCreateEachTime() {
    return segmentNumToCreateEachTime;
  }

  public void setSegmentNumToCreateEachTime(int leastSegmentUnitAtBeginning) {
    this.segmentNumToCreateEachTime = leastSegmentUnitAtBeginning;
  }

  /**
   * When new segment creation request has been sent, this method should be called so that I can
   * know the specified segment should be here soon.
   */
  public synchronized void updateVolumeLayout(int segmentIndex, boolean value) {
    if (value) {
      volumeLayoutRange.add(Range.open(segmentIndex - 1, segmentIndex + 1));
    } else {
      volumeLayoutRange.remove(Range.closed(segmentIndex, segmentIndex));
    }
    updateVolumeLayoutString();
  }

  private void updateVolumeLayoutString() {
    List<Range<Integer>> rangesToRemove = new ArrayList<Range<Integer>>();
    for (Range<Integer> range : volumeLayoutRange.asRanges()) {
      if (range.lowerBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.lowerEndpoint(), range.lowerEndpoint() + 1));
      }
      if (range.upperBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.upperEndpoint() - 1, range.upperEndpoint()));
      }
    }

    for (Range<Integer> range : rangesToRemove) {
      volumeLayoutRange.remove(range);
    }
    volumeLayoutString = volumeLayoutRange.toString();
  }

  @JsonProperty("volumeLayoutString")
  public String getVolumeLayout() {
    return volumeLayoutString;
  }

  /**
   * Set the volume layout by string, we will parse the given string to range set.
   *
   * @param volumeLayoutString the string should comes from {@code RangeSet}.toString() notice that
   *                           the range set should only contain closed ranges
   */
  public void setVolumeLayout(String volumeLayoutString) {
    this.volumeLayoutRange = ClosedRangeSetParser.parseRange(volumeLayoutString);
    this.volumeLayoutString = volumeLayoutString;
  }

  /**
   * Get the map of data node instance and volume Id. In root volume, there are some children
   * volume, some time we need to know the children volume to instruct data node to do something.
   * For example, create snapshot or rollback. In this case, we need know the children volume and
   * instance.
   */
  public HashMultimap<InstanceId, Long> mapOfDataNodeInstanceAndVolumeId(boolean withArbiters) {
    HashMultimap<InstanceId, Long> instanceWithVolumeId = HashMultimap.create();
    for (SegmentMetadata segment : this.getSegments()) {
      long volumeId = segment.getSegId().getVolumeId().getId();
      SegmentMembership membership = segment.getLatestMembership();
      instanceWithVolumeId.put(membership.getPrimary(), volumeId);
      for (InstanceId instanceId : membership.getSecondaries()) {
        instanceWithVolumeId.put(instanceId, volumeId);
      }
      for (InstanceId instanceId : membership.getJoiningSecondaries()) {
        instanceWithVolumeId.put(instanceId, volumeId);
      }
      if (withArbiters) {
        for (InstanceId instanceId : membership.getArbiters()) {
          instanceWithVolumeId.put(instanceId, volumeId);
        }
      }
    }
    return instanceWithVolumeId;
  }

  public SegId getActualSegIdBySegIndex(int segIndex) {
    // this mapLogicIndexToSegId can be init before we call this method, if not, go next option
    if (mapLogicIndexToSegId.containsKey(segIndex)) {
      return mapLogicIndexToSegId.get(segIndex);
    }

    SegmentMetadata absoluteSegment = getSegmentByIndex(segIndex);
    List<SegmentUnitMetadata> segUnits = absoluteSegment.getSegmentUnits();
    if (segUnits.size() > 0) {
      return segUnits.get(0).getSegId();
    }
    return null;
  }

  public boolean ifSegmentNeedsToBeHere(int segmentIndex) {
    return volumeLayoutRange.contains(segmentIndex);
  }

  @JsonIgnore
  public boolean isNeedToPersistVolumeLayout() {
    return needToPersistVolumeLayout;
  }

  public void setNeedToPersistVolumeLayout(boolean needToPersistVolumeLayout) {
    this.needToPersistVolumeLayout = needToPersistVolumeLayout;
  }

  @JsonIgnore
  public long getLastFixVolumeTime() {
    return lastFixVolumeTime;
  }

  /**
   * add method for unit test.
   */
  @JsonIgnore
  public void setLastFixVolumeTime(long lastFixVolumeTime) {
    this.lastFixVolumeTime = lastFixVolumeTime;
  }

  @JsonIgnore
  public void markLastFixVolumeTime() {
    this.lastFixVolumeTime = System.currentTimeMillis();
  }

  @JsonIgnore
  public void recordLogicSegIndexAndSegId(int segIndex, SegId segId) {
    this.mapLogicIndexToSegId.put(segIndex, segId);
  }

  @JsonIgnore
  public boolean isLeftSegment() {
    return leftSegment;
  }

  @JsonIgnore
  public void setLeftSegment(boolean leftSegment) {
    this.leftSegment = leftSegment;
  }

  @JsonIgnore
  public int getNextStartSegmentIndex() {
    return nextStartSegmentIndex;
  }

  @JsonIgnore
  public void setNextStartSegmentIndex(int nextStartSegmentIndex) {
    this.nextStartSegmentIndex = nextStartSegmentIndex;
  }

  @JsonIgnore
  public boolean isVolumeAvailable() {
    return (volumeStatus == VolumeStatus.Available || volumeStatus == VolumeStatus.Stable);
  }

  public ReadWriteType getReadWrite() {
    return readWrite;
  }

  public void setReadWrite(ReadWriteType readWrite) {
    this.readWrite = readWrite;
  }

  public Map<Integer, SegmentMetadata> getSegmentTable() {
    return segmentTable;
  }

  public void setSegmentTable(Map<Integer, SegmentMetadata> segmentTable) {
    this.segmentTable = segmentTable;
  }

  public RangeSet<Integer> getVolumeLayoutRange() {
    return volumeLayoutRange;
  }

  public void setVolumeLayoutRange(RangeSet<Integer> volumeLayoutRange) {
    this.volumeLayoutRange = volumeLayoutRange;
  }

  public Map<Integer, SegId> getMapLogicIndexToSegId() {
    return mapLogicIndexToSegId;
  }

  public void setMapLogicIndexToSegId(Map<Integer, SegId> mapLogicIndexToSegId) {
    this.mapLogicIndexToSegId = mapLogicIndexToSegId;
  }

  public int getPageWrappCount() {
    return pageWrappCount;
  }

  public void setPageWrappCount(int pageWrappCount) {
    this.pageWrappCount = pageWrappCount;
  }

  public int getSegmentWrappCount() {
    return segmentWrappCount;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }

  public long getMigrationSpeed() {
    return migrationSpeed;
  }

  public void setMigrationSpeed(long migrationSpeed) {
    this.migrationSpeed = migrationSpeed;
  }

  public double getMigrationRatio() {
    return migrationRatio;
  }

  public void setMigrationRatio(double migrationRatio) {
    this.migrationRatio = migrationRatio;
  }

  public long getTotalPageToMigrate() {
    return totalPageToMigrate;
  }

  public void setTotalPageToMigrate(long totalPageToMigrate) {
    this.totalPageToMigrate = totalPageToMigrate;
  }

  public long getAlreadyMigratedPage() {
    return alreadyMigratedPage;
  }

  public void setAlreadyMigratedPage(long alreadyMigratedPage) {
    this.alreadyMigratedPage = alreadyMigratedPage;
  }

  public VolumeInAction getInAction() {
    return inAction;
  }

  public void setInAction(VolumeInAction inAction) {
    this.inAction = inAction;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public void setEnableLaunchMultiDrivers(boolean enableLaunchMultiDrivers) {
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  @JsonIgnore
  public List<Integer> getAddSegmentOrder() {
    return addSegmentOrder;
  }

  @JsonIgnore
  public VolumeRebalanceInfo getRebalanceInfo() {
    return rebalanceInfo;
  }

  @JsonIgnore
  public void setRebalanceInfo(VolumeRebalanceInfo rebalanceInfo) {
    this.rebalanceInfo.deepCopy(rebalanceInfo);
  }

  public long getStableTime() {
    return stableTime;
  }

  public void setStableTime(long stableTime) {
    this.stableTime = stableTime;
  }

  public Map<Integer, SegmentMetadata> getExtendSegmentTable() {
    return extendSegmentTable;
  }

  public void setExtendSegmentTable(Map<Integer, SegmentMetadata> extendSegmentTable) {
    this.extendSegmentTable = extendSegmentTable;
  }

  @JsonIgnore
  public List<SegmentMetadata> getExtendSegments() {
    List<SegmentMetadata> segmentsMetadata = new ArrayList<>();

    for (Map.Entry<Integer, SegmentMetadata> segmentMetadataEntry : extendSegmentTable.entrySet()) {
      segmentsMetadata.add(segmentMetadataEntry.getValue());
    }
    return segmentsMetadata;
  }

  public VolumeExtendStatus getVolumeExtendStatus() {
    return volumeExtendStatus;
  }

  public void setVolumeExtendStatus(VolumeExtendStatus volumeExtendStatus) {
    this.volumeExtendStatus = volumeExtendStatus;
  }

  public long getWaitToCreateUnitTime() {
    return waitToCreateUnitTime;
  }

  public void setWaitToCreateUnitTime(long waitToCreateUnitTime) {
    this.waitToCreateUnitTime = waitToCreateUnitTime;
  }

  public String getEachTimeExtendVolumeSize() {
    return eachTimeExtendVolumeSize;
  }

  public void setEachTimeExtendVolumeSize(String eachTimeExtendVolumeSize) {
    this.eachTimeExtendVolumeSize = eachTimeExtendVolumeSize;
  }

  @JsonIgnore
  public boolean isMarkDelete() {
    return this.markDelete;
  }

  @JsonIgnore
  public void setMarkDelete(boolean markDelete) {
    this.markDelete = markDelete;
  }

  public enum VolumeSourceType {
    CREATE_VOLUME(VolumeSourceThrift.CREATE);

    private final VolumeSourceThrift volumeSourceThrift;

    VolumeSourceType(VolumeSourceThrift volumeSourceThrift) {
      this.volumeSourceThrift = volumeSourceThrift;
    }

    public VolumeSourceThrift getVolumeSourceThrift() {
      return volumeSourceThrift;
    }
  }

  public enum ReadWriteType {
    READONLY, READWRITE
  }
}