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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Random;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentMetadata.SegmentStatus;
import py.membership.SegmentMembership;
import py.test.TestBase;

public class VolumeLayoutTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(VolumeLayoutTest.class);
  private SegmentMembership membership = mock(SegmentMembership.class);

  @Ignore
  @Test
  public void testVolumeStatus() {
    VolumeMetadata volume = new VolumeMetadata(1L, 1L, 100, 1,
        VolumeType.REGULAR, 0, 0);
    volume.setSegmentNumToCreateEachTime(5);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume.setPageWrappCount(128);
    volume.setSegmentWrappCount(10);

    for (int i = 0; i < 5; i++) {
      SegmentMetadata seg = mockSegmentMetadata(i);
      volume.addSegmentMetadata(seg, membership);
      logger.debug("{}", seg.getSegmentStatus());
    }
    volume.initVolumeLayout();

    for (int i = 10; i < 15; i++) {
      SegmentMetadata seg = mockSegmentMetadata(i);
      volume.addSegmentMetadata(seg, membership);
      volume.updateVolumeLayout(i, true);
      logger.debug("{}", seg.getSegmentStatus());
    }

    volume.addSegmentMetadata(mockSegmentMetadata(90), membership);
    volume.updateVolumeLayout(90, true);

    logger.debug("volume layout {}", volume.getVolumeLayout());
    assertTrue(volume.isAllSegmentsAvailable());
  }

  @Test
  public void testParseVolumeLayout() {
    VolumeMetadata volume = new VolumeMetadata(1L, 1L, 100, 1,
        VolumeType.REGULAR, 0, 0);
    volume.setSegmentNumToCreateEachTime(5);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume.setPageWrappCount(128);
    volume.setSegmentWrappCount(10);

    for (int i = 0; i < 5; i++) {
      SegmentMetadata seg = mockSegmentMetadata(i);
      volume.addSegmentMetadata(seg, membership);
      logger.debug("{}", seg.getSegmentStatus());
    }

    for (int i = 10; i < 15; i++) {
      SegmentMetadata seg = mockSegmentMetadata(i);
      volume.addSegmentMetadata(seg, membership);
      logger.debug("{}", seg.getSegmentStatus());
    }

    volume.addSegmentMetadata(mockSegmentMetadata(90), membership);

    RangeSet<Integer> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed(0, 4));
    rangeSet.add(Range.closed(10, 14));
    rangeSet.add(Range.closed(90, 90));

    try {
      volume.setVolumeLayout(rangeSet.toString());
    } catch (Exception e) {
      logger.error("exception catch", e);
      fail(e.toString());
    }

    logger.debug("volume layout {}", volume.getVolumeLayout());
    Assert.isTrue(!volume.isAllSegmentsAvailable());
  }

  @Test
  public void testConcurrentModify() {
    final VolumeMetadata volume = new VolumeMetadata(1L, 1L, 200, 1, VolumeType.REGULAR,
        0, 0);
    volume.setSegmentNumToCreateEachTime(5);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume.setPageWrappCount(128);
    volume.setSegmentWrappCount(10);

    for (int i = 0; i < 5; i++) {
      SegmentMetadata seg = mockSegmentMetadata(i);
      volume.addSegmentMetadata(seg, membership);
      logger.debug("{}", seg.getSegmentStatus());
    }
    volume.initVolumeLayout();

    for (int i = 0; i < 100; i++) {
      UpdateThread thread = new UpdateThread(volume);
      thread.start();
    }

    while (volume.getSegmentCount() != volume.getRealSegmentCount()) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    logger.debug("volume layout {}", volume.getVolumeLayout());
    Assert.isTrue(volume.isAllSegmentsAvailable());
  }

  private SegmentMetadata mockSegmentMetadata(int index) {
    SegmentMetadata seg = mock(SegmentMetadata.class);
    when(seg.getSegmentStatus()).thenReturn(SegmentStatus.Healthy);
    when(seg.getIndex()).thenReturn(index);
    return seg;
  }

  public class UpdateThread extends Thread {
    private VolumeMetadata volume;
    private Random rand = new Random(1000);
    private boolean shutdown;

    public UpdateThread(VolumeMetadata volume) {
      this.volume = volume;
    }

    @Override
    public void run() {
      while (!volumeAllCreated()) {
        int i = rand.nextInt((int) volume.getVolumeSize());
        while (volume.getSegmentByIndex(i) != null && !volumeAllCreated()) {
          i = rand.nextInt((int) volume.getVolumeSize());
        }
        if (!shutdown) {
          volume.addSegmentMetadata(mockSegmentMetadata(i), membership);
          volume.updateVolumeLayout(i, true);
        }
      }
    }

    private boolean volumeAllCreated() {
      if (volume.getSegmentCount() == volume.getRealSegmentCount()) {
        shutdown = true;
        return true;
      }
      return false;
    }

  }

}
