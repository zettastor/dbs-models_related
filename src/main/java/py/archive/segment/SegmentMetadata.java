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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.exception.InvalidMembershipException;
import py.infocenter.common.InfoCenterConstants;
import py.instance.InstanceId;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.volume.VolumeMetadata;

public class SegmentMetadata {
  private static final Logger logger = LoggerFactory.getLogger(SegmentMetadata.class);

  private Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataTable;
  private SegId segId; // segId in the volume. It will not be changed when added to root volume.
  // Coordinator depends on this value for reading/writing;

  /**
   * Logic index in volume. When a volume is extended, child volumes are created. The logic index of
   * a segment in a child volume indicates that the position of the segment in the logic (big)
   * volume. Value range from 0 to volumeSize / segmentSize-1, when child volume add to root volume,
   * this index should be physical index in the child volume + positionOfFirstSegmentInLogicVolume
   * in volume
   */
  private int logicIndexInVolume;
  private VolumeMetadata volume;

  /* Segment status */
  private SegmentStatus segmentStatus;

  /* store logId for coordinate */
  private volatile AtomicLong logId;

  // freeRatio = (free space)/(total space)
  private double freeRatio;

  //for calc the Segment physical space
  private int writableUnitNumber;

  public SegmentMetadata(SegId segId, int index) {
    this.logicIndexInVolume = index;
    this.segId = segId;
    segmentUnitMetadataTable = new ConcurrentHashMap<>();
    this.logId = new AtomicLong(0);
  }

  public VolumeMetadata getVolume() {
    return volume;
  }

  public void setVolume(VolumeMetadata volume) {
    this.volume = volume;
  }

  public void putSegmentUnitMetadata(InstanceId instanceId,
      SegmentUnitMetadata segmentUnitMetadata) {
    segmentUnitMetadataTable.put(instanceId, segmentUnitMetadata);
  }

  public SegmentUnitMetadata getSegmentUnitMetadata(InstanceId instanceId) {
    return segmentUnitMetadataTable.get(instanceId);
  }

  public int getIndex() {
    return logicIndexInVolume;
  }

  public SegId getSegId() {
    return segId;
  }

  public void setSegId(SegId segId) {
    this.segId = segId;
  }

  public Map<InstanceId, SegmentUnitMetadata> getSegmentUnitMetadataTable() {
    return segmentUnitMetadataTable;
  }

  public void setSegmentUnitMetadataTable(
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataTable) {
    this.segmentUnitMetadataTable = segmentUnitMetadataTable;
  }

  public SegmentStatus getSegmentStatus() {
    int primaryCount = 0;
    int secondaryCount = 0;
    int joiningSecondaryCount = 0;

    int availablePrimary = 0;
    int availableSecondary = 0;
    int availableArbiter = 0;
    int availableJoiningSecondary = 0;
    int availableJoiningArbiter = 0;
    int deletingPrimary = 0;
    int deletingSecondary = 0;
    int deletedPrimary = 0;
    int deletedSecondary = 0;

    logger.info("to get the volume :{} status", volume.getVolumeId());
    // filter the timeout segment unit first
    for (Iterator<SegmentUnitMetadata> it = segmentUnitMetadataTable.values().iterator();
        it.hasNext(); ) {
      SegmentUnitMetadata segUnit = it.next();
      // check segment unit is timeout
      if (System.currentTimeMillis() - segUnit.getLastReported() >= TimeUnit.SECONDS
          .toMillis(InfoCenterConstants.getSegmentUnitReportTimeout())) {
        logger.debug(
            "this segment unit report time is time out and will be removed, segment unit is {}",
            segUnit);
        segmentUnitMetadataTable.remove(segUnit.getInstanceId());
      }
    }

    // choose the latest segment unit data
    Collection<SegmentUnitMetadata> latestSegUnits = chooseLatestSegUnits();
    Map<SegmentUnitMetadata, InstanceId> segUnitToInstance = SegmentUnitMetadata
        .reverse(this.getSegmentUnitMetadataTable());
    SegmentMembership latestMembership = null;
    for (SegmentUnitMetadata segUnit : latestSegUnits) {
      SegmentMembership membership = segUnit.getMembership();
      if (latestMembership == null) {
        latestMembership = membership;
      } else {
        try {
          if (latestMembership.compareTo(membership) < 0) {
            latestMembership = membership;
          }
        } catch (InvalidMembershipException e) {
          logger.warn("caught an exception when get latest membership", e);
        }
      }
      SegmentUnitStatus status = segUnit.getStatus();
      InstanceId id = segUnitToInstance.get(segUnit);

      if (status == SegmentUnitStatus.Primary && membership.isPrimary(id)) {
        primaryCount++;
        availablePrimary++;
      } else if (status == SegmentUnitStatus.PrePrimary && membership.isPrimary(id)) {
        /* is the Primary is PrePrimary, is availableSecondary  ***/
        primaryCount++;
        availableSecondary++;
      } else if (status == SegmentUnitStatus.Secondary && membership.isSecondary(id)) {
        secondaryCount++;
        availableSecondary++;
      } else if (status == SegmentUnitStatus.Arbiter && membership.isArbiter(id)) {
        availableArbiter++;
      } else if (status == SegmentUnitStatus.Deleting) {
        logger.debug("Seg primary[{}] unit[{}] isPrimary[{}] deleting", membership.getPrimary(), id,
            membership.isPrimary(id));

        if (membership.isPrimary(id)) {
          deletingPrimary++;
        } else if (membership.isAliveSecondaries(id)) {
          deletingSecondary++;
        }
      } else if (status == SegmentUnitStatus.Deleted) {
        logger.debug("Seg primary [{}] unit[{}] isPrimary[{}] deleted", membership.getPrimary(), id,
            membership.isPrimary(id));

        if (membership.isPrimary(id)) {
          deletedPrimary++;
        } else if (membership.isAliveSecondaries(id)) {
          deletedSecondary++;
        }
      } else if (status == SegmentUnitStatus.OFFLINING || status == SegmentUnitStatus.OFFLINED
          || status == SegmentUnitStatus.Unknown
          || status == SegmentUnitStatus.Broken) {
        // do nothing, just take position, ensure not go to follow else if
      } else if (membership.isArbiter(id)) {
        if (status == SegmentUnitStatus.ModeratorSelected
            || status == SegmentUnitStatus.SecondaryEnrolled
            || status == SegmentUnitStatus.SecondaryApplicant
            || status == SegmentUnitStatus.PreArbiter) {
          // this arbiter can support the primary
          availableJoiningArbiter++;
        }
      } else if (membership.isJoiningSecondary(id)) {
        if (status == SegmentUnitStatus.ModeratorSelected
            || status == SegmentUnitStatus.SecondaryEnrolled
            || status == SegmentUnitStatus.SecondaryApplicant
            || status == SegmentUnitStatus.PreSecondary
            // could be secondary status
            || status == SegmentUnitStatus.Secondary) {
          // this secondary can support the primary
          availableJoiningSecondary++;
        }

        if (status != SegmentUnitStatus.Start) {
          joiningSecondaryCount++;
        }
      } else if (membership.isPrimary(id)) {
        if (status != SegmentUnitStatus.Start) {
          primaryCount++;
        }
      } else if (membership.isSecondary(id)) {
        if (status != SegmentUnitStatus.Start) {
          secondaryCount++;
        }
      }
    } // latestSegUnits

    //calc the segment number
    writableUnitNumber = primaryCount + secondaryCount + joiningSecondaryCount;
    logger.info("when getSegmentStatus, volume:{}, segid :{} get the writableUnitNumber is :{}",
        segId.getVolumeId().getId(), segId.getIndex(), writableUnitNumber);

    SegmentForm segmentForm = null;
    if (latestMembership != null) {
      segmentForm = SegmentForm.getSegmentForm(latestMembership, this.volume.getVolumeType());
    }

    if ((availablePrimary + availableSecondary + availableArbiter) == this.volume
        .getVolumeType()
        .getNumMembers()) {
      logger.debug(
          "current segment status is available, avialablePrimay is {}, availableSecondary is {}",
          availablePrimary, availableSecondary);
      this.segmentStatus = SegmentStatus.Healthy;
    } else if (availablePrimary == 1
        && (availableSecondary + availableArbiter + availableJoiningArbiter
        + availableJoiningSecondary) >= this.volume.getVolumeType().getVotingQuorumSize() - 1) {
      logger.debug(
          "there are some members absent: avialablePrimay is {}, availableSecondary is {}, "
              + "availableArbiter {} "
              + "availableJoiningSecondary is {} and availableJoiningArbiter is {}",
          availablePrimary, availableSecondary, availableArbiter, availableJoiningSecondary,
          availableJoiningArbiter);
      if (availableJoiningSecondary > 0 || availableJoiningArbiter > 0) {
        this.segmentStatus = SegmentStatus.Recovering;
      } else {
        this.segmentStatus = SegmentStatus.Degraded;
      }
    } else if (deletingPrimary >= 1
        && deletingSecondary >= this.volume.getVolumeType().getVotingQuorumSize() - 1) {
      // if there is one deleting primary and (quorum-1) deleting secondary, then this segment is
      // deleting
      logger
          .debug("this segment is become deleting: deletingPrimary is {}, deleting secondary is {}",
              deletingPrimary, deletingSecondary);
      this.segmentStatus = SegmentStatus.Deleting;
    } else if (deletedPrimary + deletedSecondary >= this.volume.getVolumeType()
        .getWriteQuorumSize()) {
      /* if the deletedPrimary + deletedSecondary >= Quorum, so we think the segment is Dead ***/
      logger.info("now the volume cannot be recovered after deleting one segment");
      this.segmentStatus = SegmentStatus.Dead;
    } else if (segmentForm != null && segmentForm
        .writable(primaryCount, secondaryCount, joiningSecondaryCount, volume.getVolumeType())) {
      this.segmentStatus = SegmentStatus.Writable;
      logger.info("now the segment status is :{}", this.segmentStatus);
    } else {
      this.segmentStatus = SegmentStatus.Unavailable;
    }
    return this.segmentStatus;
  }

  /**
   * when all datanode down, and delete the volume, when timeout, all segment is empty so i can set
   * the volume status to dead.
   */
  public boolean checkCurrentSegmentIsEnpty() {
    if (segmentUnitMetadataTable.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Change segment status is changed since last time we get segment status.
   */
  public boolean isSegmentStatusChanged() {
    SegmentStatus oldStatus = this.segmentStatus;
    return (getSegmentStatus() != oldStatus);
  }

  /**
   *  This function first decides the highest membership, and then picks up these members
   *  from the existing SegmentMetadatas that were reported by datnaodes. This function may
   *  return an empty set when there is no any segment metadata in the highest membership or
   *  all segment metadatas are timed out and removed.
   */
  public Collection<SegmentUnitMetadata> chooseLatestSegUnits() {
    Set<SegmentUnitMetadata> latestSegUnits = new HashSet<>();
    if (this.getSegmentUnitCount() == 0) {
      // no segment unit exist or all of them are timed out
      return latestSegUnits;
    }

    List<SegmentUnitMetadata> segUnitsList = new ArrayList<>(segmentUnitMetadataTable.values());
    // sort the segUnitsList by membership from old to new
    Collections.sort(segUnitsList);
    // reverse the order, the first one is the latest membership
    Collections.reverse(segUnitsList);
    SegmentMembership highestMemberShip = segUnitsList.get(0).getMembership();

    // get primary
    InstanceId primaryId = highestMemberShip.getPrimary();
    for (SegmentUnitMetadata segUnitMetadata : segUnitsList) {
      if (segUnitMetadata.getInstanceId().equals(primaryId)) {
        latestSegUnits.add(segUnitMetadata);
        break;
      }
    }

    // get alive secondaries, including secondaries, arbiters, and joining secondaries
    Set<InstanceId> aliveSecondaries = highestMemberShip.getAliveSecondaries();
    for (InstanceId secondaryId : aliveSecondaries) {
      for (SegmentUnitMetadata segUnitMetadata : segUnitsList) {
        if (secondaryId.equals(segUnitMetadata.getInstanceId())) {
          latestSegUnits.add(segUnitMetadata);
          break;
        }
      }
    }

    return latestSegUnits;
  }

  /**
   * Get all the segment units in segment.
   */
  public List<SegmentUnitMetadata> getSegmentUnits() {
    return new ArrayList<>(segmentUnitMetadataTable.values());
  }

  /**
   * Get segment unit count.
   */
  public int getSegmentUnitCount() {
    return this.segmentUnitMetadataTable.size();
  }

  public SegmentMembership getLatestMembership() {
    SegmentMembership highestMembershipInSegment = null;
    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segmentUnitMetadataTable.entrySet()) {
      SegmentUnitMetadata segUnitMetadata = entry.getValue();
      SegmentMembership currentMembership = segUnitMetadata.getMembership();
      try {
        if (currentMembership.compareTo(highestMembershipInSegment) > 0) {
          highestMembershipInSegment = currentMembership;
        }
      } catch (InvalidMembershipException e) {
        logger.warn("caught an exception when get latest membership", e);
      }
    }
    return new SegmentMembership(highestMembershipInSegment);
  }

  @Override
  public String toString() {
    StringBuilder segInfo = new StringBuilder();
    segInfo.append("SegmentMetadata index=" + logicIndexInVolume + ", SegId=" + this.segId
        + " , SegmentStatus=" + this.segmentStatus + "] \n");
    segInfo.append("SegmentUnitMetadataInfo as follow:\n");

    for (Entry<InstanceId, SegmentUnitMetadata> entry : segmentUnitMetadataTable.entrySet()) {
      segInfo.append("InstanceId=").append(entry.getKey().toString()).append(" ")
          .append(entry.getValue().toString()).append("\n");
    }

    return segInfo.toString();
  }

  public void setLogId(Long logId) {
    this.logId.set(logId);
  }

  public double getFreeRatio() {
    return freeRatio;
  }

  public void setFreeRatio(double freeRatio) {
    this.freeRatio = freeRatio;
  }

  public long getAndIncLogId() {
    return logId.incrementAndGet();
  }

  public long getAndAddLogId(int count) {
    return logId.addAndGet(count);
  }

  public int getWritableUnitNumber() {
    return writableUnitNumber;
  }

  public static enum SegmentStatus {
    Healthy(
        1),
    // the number of members in the membership equals the numMembers in the predefined volume type
    Degraded(
        2),
    // the number of alive members in the membership excluding inactive secondaries is
    // less than the numMembers in the predefined volume type
    Recovering(3), // there is a joining secondary in the membership
    Unavailable(4), // the number of alive members in the membership is less than the voting quorum
    Deleting(5), // the majority of alive members are being deleting
    Dead(6),
    Writable(7);  // this segment still can be written, should mark its available for now
    private final int value;

    private SegmentStatus(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public boolean available() {
      return Healthy.equals(this) || Degraded.equals(this) || Recovering.equals(this) || Writable
          .equals(this);
    }

    public boolean isStable() {
      return Healthy.equals(this);
    }
  }
}
