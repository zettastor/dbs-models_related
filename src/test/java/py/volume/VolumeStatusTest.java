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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.infocenter.common.InfoCenterConstants;
import py.instance.InstanceId;
import py.test.TestBase;
import py.test.TestUtils;

/**
 * This test unit is to test the volume status transition.
 *
 */
public class VolumeStatusTest extends TestBase {
  VolumeMetadata volume = null;
  SegmentMetadata seg1 = null;
  SegmentMetadata seg2 = null;

  //segment units belong to first segment;
  SegmentUnitMetadata primaryUnit1 = null;
  SegmentUnitMetadata secondaryUnit11 = null;
  SegmentUnitMetadata secondaryUnit12 = null;

  //segment units belong to second segment;
  SegmentUnitMetadata primaryUnit2 = null;
  SegmentUnitMetadata secondaryUnit21 = null;
  SegmentUnitMetadata secondaryUnit22 = null;

  /**
   * For every test case begin, create a volume with two segments. Every segment includes  three
   * segment units with OK status ;
   */
  @Before
  public void setUp() throws Exception {
    volume = TestUtils.generateVolumeMetadata();
    //two segments
    volume.setVolumeSize(32);
    volume.setSegmentSize(16);
    volume.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    seg1 = volume.getSegmentByIndex(0);
    seg2 = volume.getSegmentByIndex(1);

    InstanceId primaryId = new InstanceId(1);
    InstanceId secondaryId1 = new InstanceId(2);
    InstanceId secondaryId2 = new InstanceId(3);
    primaryUnit1 = seg1.getSegmentUnitMetadata(primaryId);
    secondaryUnit11 = seg1.getSegmentUnitMetadata(secondaryId1);
    secondaryUnit12 = seg1.getSegmentUnitMetadata(secondaryId2);

    primaryUnit2 = seg2.getSegmentUnitMetadata(primaryId);
    secondaryUnit21 = seg2.getSegmentUnitMetadata(secondaryId1);
    secondaryUnit22 = seg2.getSegmentUnitMetadata(secondaryId2);
  }

  /**
   * normal status transfer: From volume creating to dead.
   */
  @Test
  public void testVolumeStatusNormalTransition() {
    //When create a volume, it is in ToBeCreated state;
    VolumeMetadata volumeTest = new VolumeMetadata(111, 111, 32, 16, VolumeType.REGULAR,
        0L, 0L);
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.ToBeCreated);
    volumeTest.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeTest.setVolumeCreatedTime(new Date());

    //Once a segment come in, it is in creating state
    volumeTest.addSegmentMetadata(seg1, primaryUnit1.getMembership());
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Creating);

    //All segment is OK, it is in available state
    volumeTest.addSegmentMetadata(seg2, primaryUnit2.getMembership());
    volumeTest.updateStatus();
    assertTrue(volumeTest.isVolumeAvailable());

    //a segment is unavailable, the volume is unavailable
    primaryUnit1.setStatus(SegmentUnitStatus.Unknown);
    secondaryUnit11.setStatus(SegmentUnitStatus.Broken);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Unavailable);

    //from unavailable to available
    primaryUnit1.setStatus(SegmentUnitStatus.Primary);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Available);

    //from available to deleting
    volumeTest.setVolumeStatus(VolumeStatus.Deleting);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleting);

    //from deleting to deleted
    primaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit12.setStatus(SegmentUnitStatus.Deleting);
    primaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit21.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit22.setStatus(SegmentUnitStatus.Deleting);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Deleting);
    assertEquals(seg2.getSegmentStatus(), SegmentMetadata.SegmentStatus.Deleting);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleted);

    //from deleted to dead
    primaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);

    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit12.setStatus(SegmentUnitStatus.Deleted);
    primaryUnit2.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit21.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit22.setStatus(SegmentUnitStatus.Deleted);
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);
  }

  /**
   * from toBeCreated status to different status.
   */
  @Test
  public void testVolumeStatusFromToBeCreated() {
    //from ToBeCreated to Deleting;
    VolumeMetadata volumeTest = new VolumeMetadata();
    volumeTest.setVolumeSize(100);
    volumeTest.setSegmentSize(10);
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.ToBeCreated);

    volumeTest.setVolumeStatus(VolumeStatus.Deleting);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);

    //from ToBeCreated to timeout, then it will turn to deleting, then deleted, then dead
    volumeTest.setVolumeStatus(VolumeStatus.ToBeCreated);
    volumeTest.setVolumeCreatedTime(new Date());
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.ToBeCreated);
    volumeTest.setVolumeCreatedTime(new Date(1));
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);
  }

  /**
   * from Creating to different status.
   */
  @Test
  public void testVolumeStatusFromCreating() {
    VolumeMetadata volumeTest = new VolumeMetadata();
    volumeTest.setSegmentSize(16);
    volumeTest.setVolumeSize(32);
    volumeTest.setVolumeType(VolumeType.REGULAR);
    volumeTest.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeTest.addSegmentMetadata(seg1, primaryUnit1.getMembership());
    volumeTest.addSegmentMetadata(seg2, primaryUnit2.getMembership());
    volumeTest.updateStatus();
    assertTrue(volumeTest.isVolumeAvailable());

    volumeTest.setVolumeStatus(VolumeStatus.Deleting);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleting);

    primaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit12.setStatus(SegmentUnitStatus.Deleting);

    primaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit21.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit22.setStatus(SegmentUnitStatus.Deleting);

    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleted);

    primaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);

  }

  /**
   * from Creating to deleting, when time is out.
   */
  @Test
  public void testVolumeStatusFromCreatingTimeout() {
    VolumeMetadata volumeTest = new VolumeMetadata();
    volumeTest.setSegmentSize(16);
    volumeTest.setVolumeSize(32);
    volumeTest.setVolumeType(VolumeType.REGULAR);
    volumeTest.addSegmentMetadata(seg1, primaryUnit1.getMembership());
    volumeTest.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeTest.setVolumeCreatedTime(new Date());

    volumeTest.updateStatus();
    primaryUnit1.setStatus(SegmentUnitStatus.PrePrimary);
    secondaryUnit11.setStatus(SegmentUnitStatus.ModeratorSelected);
    secondaryUnit12.setStatus(SegmentUnitStatus.ModeratorSelected);

    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Creating);

    // from Creating to timeout, then it will turn to deleting, then deleted, then dead
    volumeTest.setVolumeCreatedTime(new Date(1));
    primaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit21.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit22.setStatus(SegmentUnitStatus.Deleting);
    volumeTest.addSegmentMetadata(seg2, primaryUnit2.getMembership());

    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleting);

    primaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit12.setStatus(SegmentUnitStatus.Deleting);

    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Deleted);

    primaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volumeTest.updateStatus();
    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);
  }

  @Test
  public void testVolumeStatusFromAvailable() {
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Available);

    //from available to unavailable
    primaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Available);

    primaryUnit1.setStatus(SegmentUnitStatus.Start);
    secondaryUnit11.setStatus(SegmentUnitStatus.Unknown);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    secondaryUnit12.setStatus(SegmentUnitStatus.Deleting);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    //from unavailable to available
    primaryUnit1.setStatus(SegmentUnitStatus.PrePrimary);
    secondaryUnit11.setStatus(SegmentUnitStatus.Start);
    assertEquals(seg1.getSegmentStatus(), SegmentMetadata.SegmentStatus.Unavailable);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    //from avaiable to deleting
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Deleting);

    //from deleting to dead
    primaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Dead);
  }

  @Test
  public void testVolumeStatusFromDeleting() {
    volume.setVolumeStatus(VolumeStatus.Deleting);
    primaryUnit1.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit12.setStatus(SegmentUnitStatus.Deleting);
    primaryUnit2.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit21.setStatus(SegmentUnitStatus.Deleting);
    secondaryUnit22.setStatus(SegmentUnitStatus.Deleting);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Deleted);

    primaryUnit1.setStatus(SegmentUnitStatus.Deleted);
    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Dead);
  }

  @Test
  public void testVolumeStatusFromFixingToAvailable() {
    primaryUnit1.setStatus(SegmentUnitStatus.Start);
    secondaryUnit11.setStatus(SegmentUnitStatus.OFFLINED);
    secondaryUnit12.setStatus(SegmentUnitStatus.Broken);

    primaryUnit2.setStatus(SegmentUnitStatus.PrePrimary);
    secondaryUnit21.setStatus(SegmentUnitStatus.Start);
    secondaryUnit22.setStatus(SegmentUnitStatus.Unknown);

    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    volume.setVolumeStatus(VolumeStatus.Fixing);

    secondaryUnit11.setStatus(SegmentUnitStatus.Start);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

  }

  @Test
  public void testVolumeStatusFromFixingToUnavailableTimeout() {
    primaryUnit1.setStatus(SegmentUnitStatus.Start);
    secondaryUnit11.setStatus(SegmentUnitStatus.OFFLINED);
    secondaryUnit12.setStatus(SegmentUnitStatus.Broken);

    primaryUnit2.setStatus(SegmentUnitStatus.PrePrimary);
    secondaryUnit21.setStatus(SegmentUnitStatus.Start);
    secondaryUnit22.setStatus(SegmentUnitStatus.Unknown);

    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    volume.setVolumeStatus(VolumeStatus.Fixing);

    volume.setLastFixVolumeTime(0);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    secondaryUnit11.setStatus(SegmentUnitStatus.Start);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);

    secondaryUnit11.setStatus(SegmentUnitStatus.Deleted);
    volume.updateStatus();
    assertEquals(volume.getVolumeStatus(), VolumeStatus.Unavailable);
  }

  /**
   * from Creating to deleting, when time is out.
   */
  @Test
  public void testVolumeStatusFromCreatingTimeout_New() throws InterruptedException {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);

    VolumeMetadata volumeTest = new VolumeMetadata();
    volumeTest.setSegmentSize(16);
    volumeTest.setVolumeSize(32);
    volumeTest.setVolumeType(VolumeType.REGULAR);
    volumeTest.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeTest.setVolumeCreatedTime(new Date());

    InfoCenterConstants.setVolumeToBeCreatedTimeout(5);
    InfoCenterConstants.setVolumeBeCreatingTimeout(5);
    //ToBeCreated time out
    for (int i = 0; i < 6; i++) {
      Thread.sleep(1000);
      logger.warn("---- sleep -- 1000");
      volumeTest.updateStatus();
    }

    //Deleting time out
    for (int i = 0; i < 6; i++) {
      Thread.sleep(1000);
      logger.warn("---- sleep -- 1000");
      volumeTest.updateStatus();
    }

    assertEquals(volumeTest.getVolumeStatus(), VolumeStatus.Dead);
  }
}
