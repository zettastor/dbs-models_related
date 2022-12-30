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

package py.archive.segment.recurring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentMetadata.SegmentStatus;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.storage.impl.DummyStorage;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * Test the change of segment status.
 */
public class SegmentStatusTest extends TestBase {
  VolumeMetadata volume = null;
  SegmentMetadata seg1 = null;
  SegmentMetadata seg2 = null;
  SegmentUnitMetadata primaryUnit = null;
  SegmentUnitMetadata secondaryUnit1 = null;
  SegmentUnitMetadata secondaryUnit2 = null;

  VolumeMetadata smallVolume = null;
  SegmentMetadata smallSeg1 = null;
  SegmentMetadata smallSeg2 = null;
  SegmentUnitMetadata smallPrimaryUnit = null;
  SegmentUnitMetadata smallSecondaryUnit1 = null;
  SegmentUnitMetadata smallSecondaryUnit2 = null;

  public static SegmentMetadata generateSegmentMetadataForPrePrimary(SegId segId,
      VolumeType volumeType) {
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());
    SegmentUnitMetadata segmentUnitMetadataPrimary = generateSegmentUnitMetadata(segId,
        SegmentUnitStatus.PrePrimary, 0,
        volumeType, SegmentUnitType.Normal);

    segmentUnitMetadataPrimary.setLastReported(System.currentTimeMillis());
    // get the primary id from the membership
    SegmentMembership currentMembership = segmentUnitMetadataPrimary.getMembership();
    InstanceId primaryId = currentMembership.getPrimary();
    segmentUnitMetadataPrimary.setInstanceId(primaryId);
    segmentMetadata.putSegmentUnitMetadata(primaryId, segmentUnitMetadataPrimary);

    ArrayList<InstanceId> secondariesId = new ArrayList<>(
        currentMembership.getJoiningSecondaries());
    for (int i = 0; i < currentMembership.getJoiningSecondaries().size(); i++) {
      SegmentUnitMetadata segmentUnitMetadataSecondary = generateSegmentUnitMetadata(segId,
          SegmentUnitStatus.Secondary, 0, volumeType,
          SegmentUnitType.Normal);
      segmentUnitMetadataSecondary.setLastReported(System.currentTimeMillis());
      segmentUnitMetadataSecondary.setInstanceId(new InstanceId(secondariesId.get(i)));
      segmentMetadata.putSegmentUnitMetadata(new InstanceId(secondariesId.get(i)),
          segmentUnitMetadataSecondary);
    }

    return segmentMetadata;
  }

  public static SegmentUnitMetadata generateSegmentUnitMetadata(SegId segId,
      SegmentUnitStatus status,
      long startOffsetInArchive, VolumeType volumeType, SegmentUnitType unitType) {
    SegmentUnitMetadata newMetadata = new SegmentUnitMetadata(segId, startOffsetInArchive,
        generateMembershipForPrePrimary(), status, volumeType, unitType);
    Storage storage = new DummyStorage("dummpy Storage", 1);
    newMetadata.setStorage(storage);
    newMetadata.setMetadataOffsetInArchive(0);
    newMetadata.setStatus(status);
    //newMetadata.updateSnapshotManager(new GenericVolumeSnapshotManager(0L), false);
    return newMetadata;
  }

  public static SegmentMembership generateMembershipForPrePrimary() {
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> joinSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<>();

    long primaryId = 1L;
    long secondaryId = primaryId;
    secondaryId++;
    joinSecondaries.add(new InstanceId(secondaryId));

    return new SegmentMembership(new SegmentVersion(1, 1), new InstanceId(primaryId),
        new InstanceId(primaryId),
        secondaries, arbiters, null, joinSecondaries, null, null);
  }

  @Before
  public void setUp() throws Exception {
    volume = TestUtils.generateVolumeMetadata();
    seg1 = volume.getSegmentByIndex(0);
    seg2 = volume.getSegmentByIndex(1);

    assertTrue(seg1.getSegmentStatus().available());
    assertTrue(seg2.getSegmentStatus().available());

    SegmentMembership seg1Membership = seg1.getLatestMembership();
    primaryUnit = seg1.getSegmentUnitMetadata(seg1Membership.getPrimary());
    Iterator<InstanceId> secondariesIdIterator = seg1Membership.getSecondaries().iterator();
    secondaryUnit1 = seg1.getSegmentUnitMetadata(secondariesIdIterator.next());
    secondaryUnit2 = seg1.getSegmentUnitMetadata(secondariesIdIterator.next());

    smallVolume = TestUtils.generateVolumeMetadata(VolumeType.SMALL);
    smallSeg1 = smallVolume.getSegmentByIndex(0);
    smallSeg2 = smallVolume.getSegmentByIndex(1);

    assertTrue(smallSeg1.getSegmentStatus().available());
    assertTrue(smallSeg2.getSegmentStatus().available());

    smallPrimaryUnit = smallSeg1.getSegmentUnitMetadata(seg1Membership.getPrimary());
    seg1Membership = smallSeg1.getLatestMembership();
    logger.debug("seg1 membership {} ", seg1Membership);
    secondariesIdIterator = seg1Membership.getSecondaries().iterator();
    Iterator<InstanceId> arbiterIdIterator = seg1Membership.getArbiters().iterator();
    smallSecondaryUnit1 = smallSeg1.getSegmentUnitMetadata(secondariesIdIterator.next());
    smallSecondaryUnit2 = smallSeg1.getSegmentUnitMetadata(arbiterIdIterator.next());

    assertEquals(SegmentUnitType.Normal, smallSecondaryUnit1.getSegmentUnitType());
    assertEquals(SegmentUnitType.Arbiter, smallSecondaryUnit2.getSegmentUnitType());
  }

  @Test
  public void testSegmentStatusFromAvailableToUnavailable() {
    assertTrue(seg1.getSegmentStatus().available());
    assertTrue(seg2.getSegmentStatus().available());

    primaryUnit.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Writable);
    primaryUnit.setStatus(SegmentUnitStatus.Primary);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Healthy);

    secondaryUnit1.setStatus(SegmentUnitStatus.Unknown);
    secondaryUnit2.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Unavailable);

    secondaryUnit1.setStatus(SegmentUnitStatus.Secondary);
    secondaryUnit2.setStatus(SegmentUnitStatus.Secondary);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Healthy);

    secondaryUnit2.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Degraded);

    primaryUnit.setStatus(SegmentUnitStatus.OFFLINED);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Unavailable);
  }

  @Test
  public void testSegmentStatusFromAvailableToUnavailableInSmallVolume() {
    assertEquals(SegmentMetadata.SegmentStatus.Healthy, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());
    smallPrimaryUnit.setStatus(SegmentUnitStatus.Primary);
    assertEquals(SegmentMetadata.SegmentStatus.Healthy, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(SegmentMetadata.SegmentStatus.Degraded, smallSeg1.getSegmentStatus());
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(SegmentMetadata.SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Secondary);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Arbiter);
    assertEquals(SegmentMetadata.SegmentStatus.Healthy, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.Unknown);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Unknown);
    assertEquals(SegmentStatus.Unavailable, smallSeg1.getSegmentStatus());
  }

  @Test
  public void testSegmentStatusAvaiableToDeleting() {
    assertTrue(seg1.getSegmentStatus().available());
    primaryUnit.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentStatus.Writable, seg1.getSegmentStatus());

    secondaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentMetadata.SegmentStatus.Deleting, seg1.getSegmentStatus());

    secondaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentMetadata.SegmentStatus.Deleting, seg1.getSegmentStatus());
  }

  @Test
  public void testSegmentStatusAvaiableToDeletingInSmallVolume() {
    smallPrimaryUnit.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentMetadata.SegmentStatus.Deleting, smallSeg1.getSegmentStatus());

    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentMetadata.SegmentStatus.Deleting, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.PrePrimary);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.PreArbiter);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.Deleting);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.ModeratorSelected);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.PrePrimary);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.PreSecondary);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());
  }

  @Test
  public void testSegmentStatusFromDeletingToDead() {
    secondaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(SegmentStatus.Degraded, seg1.getSegmentStatus());

    secondaryUnit2.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Dead);

    secondaryUnit1.setStatus(SegmentUnitStatus.Secondary);
    secondaryUnit2.setStatus(SegmentUnitStatus.Secondary);
    primaryUnit.setStatus(SegmentUnitStatus.Primary);
    assertTrue(seg1.getSegmentStatus().available());

    // check only primary unit in Deleted
    primaryUnit.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Writable);

    //two
    secondaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Dead);

    //three
    secondaryUnit2.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Dead);
  }

  @Test
  public void testSegmentStatusFromDeletingToDeadInSmallVolume() {
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(SegmentStatus.Degraded, smallSeg1.getSegmentStatus());

    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(SegmentMetadata.SegmentStatus.Dead, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Secondary);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Arbiter);
    assertEquals(SegmentStatus.Healthy, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.Deleted);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(smallSeg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Dead);

    // check only primary unit in Deleted
    smallPrimaryUnit.setStatus(SegmentUnitStatus.Primary);
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Secondary);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Arbiter);
    assertEquals(SegmentStatus.Healthy, smallSeg1.getSegmentStatus());

    smallPrimaryUnit.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(smallSeg1.getSegmentStatus(), SegmentStatus.Writable);
  }

  @Test
  public void testSegmentStatusFromAvailableToRecovering() {
    // change membership to <P, S, Joining S>
    SegmentMembership membership = primaryUnit.getMembership();
    Iterator<InstanceId> secondariesIdIterator = membership.getSecondaries().iterator();
    InstanceId secondaryId = secondariesIdIterator.next();
    SegmentMembership newMembership = membership.removeSecondary(secondaryId);
    membership = newMembership.addJoiningSecondary(secondaryId);

    primaryUnit.setMembership(membership);
    secondaryUnit1.setMembership(membership);
    secondaryUnit2.setMembership(membership);

    assertEquals(SegmentStatus.Recovering, seg1.getSegmentStatus());

    // no matter what status of joining secondary in the membership is,
    // it is counted as a joining secondary
    secondaryUnit1.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Degraded, seg1.getSegmentStatus());
  }

  @Test
  public void testSegmentStatusFromAvailableToRecoveringInSmallVolume() {
    // set the arbiter's status to Start
    assertEquals(SegmentUnitStatus.Arbiter, smallSecondaryUnit2.getStatus());
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Degraded, smallSeg1.getSegmentStatus());

    smallSecondaryUnit2.setStatus(SegmentUnitStatus.PreArbiter);
    assertEquals(SegmentStatus.Recovering, smallSeg1.getSegmentStatus());

    // change membership to <P, Joining S, Arbiter>
    SegmentMembership membership = smallPrimaryUnit.getMembership();
    Iterator<InstanceId> secondariesIdIterator = membership.getSecondaries().iterator();
    InstanceId secondaryId = secondariesIdIterator.next();
    SegmentMembership newMembershipWoSecondary = membership.removeSecondary(secondaryId);
    membership = newMembershipWoSecondary.addJoiningSecondary(secondaryId);

    smallPrimaryUnit.setMembership(membership);
    smallSecondaryUnit1.setMembership(membership);
    smallSecondaryUnit2.setMembership(membership);
    // since the membership is <P, Arbiter, Joining Secondary> and Aribter's status is PreArbiter
    // so the status is recorvering
    assertEquals(SegmentStatus.Recovering, smallSeg1.getSegmentStatus());

    // no matter what status of joining secondary in the membership is,
    // it is counted as a joining secondary
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Recovering, smallSeg1.getSegmentStatus());
    smallSecondaryUnit1.setStatus(SegmentUnitStatus.PreSecondary);
    assertEquals(SegmentStatus.Recovering, smallSeg1.getSegmentStatus());

    // Make the membership to <P, Arbiter>
    smallPrimaryUnit.setMembership(newMembershipWoSecondary);
    smallSecondaryUnit1.setMembership(newMembershipWoSecondary);
    smallSecondaryUnit2.setMembership(newMembershipWoSecondary);
    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallSecondaryUnit2.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    // Make the membership to <P, Joining Secondary>
    SegmentMembership membershipWoArbiter = membership
        .removeArbiter(membership.getArbiters().iterator().next());

    SegmentMembership membershipOnlyHasPrimary = membershipWoArbiter
        .joiningSecondaryBecomeInactive(secondaryId)
        .removeInactiveSecondary(secondaryId);

    smallPrimaryUnit.setMembership(membershipOnlyHasPrimary.addJoiningSecondary(secondaryId));
    smallSecondaryUnit1.setMembership(membershipOnlyHasPrimary.addJoiningSecondary(secondaryId));
    smallSecondaryUnit2.setMembership(membershipOnlyHasPrimary.addJoiningSecondary(secondaryId));

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Start);
    assertEquals(SegmentStatus.Writable, smallSeg1.getSegmentStatus());

    smallSecondaryUnit1.setStatus(SegmentUnitStatus.Broken);
    smallPrimaryUnit.setStatus(SegmentUnitStatus.OFFLINED);
    assertEquals(SegmentStatus.Unavailable, smallSeg1.getSegmentStatus());
  }

  @Test
  public void testSegmentStatusPrimaryReportTimeout() {
    primaryUnit.setLastReported(0);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Writable);
    assertTrue(seg1.getSegmentStatus().available());
    SegmentUnitMetadata segUnit = seg1.getSegmentUnitMetadata(primaryUnit.getInstanceId());
    assertEquals(segUnit, null);
  }

  @Test
  public void testSegmentStatusSecodaryReportTimeout() {
    secondaryUnit1.setLastReported(0);
    assertEquals(seg1.getSegmentStatus(), SegmentStatus.Degraded);
    assertTrue(seg1.getSegmentStatus().available());
    secondaryUnit2.setLastReported(0);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Unavailable);
  }

  @Test
  public void testSegmentStatusFromPrePrimaryAndJoinSecond() {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setAccountId(11);
    volumeMetadata.setVolumeId(22);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());
    volumeMetadata.setName("volume_test");
    volumeMetadata.setVolumeSize(0);
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setVersion(1);

    //generate PrePrimary + join second in membership
    SegmentMetadata segmentMetadata = generateSegmentMetadataForPrePrimary(new SegId(22L, 0),
        VolumeType.REGULAR);
    volumeMetadata.addSegmentMetadata(segmentMetadata, segmentMetadata.getLatestMembership());

    //one PrePrimary + join second
    segmentMetadata.getSegmentStatus();
    assertEquals(SegmentMetadata.SegmentStatus.Writable, segmentMetadata.getSegmentStatus());
  }
}
