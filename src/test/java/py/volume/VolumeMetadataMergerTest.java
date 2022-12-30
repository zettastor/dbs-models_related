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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import org.junit.Test;
import py.test.TestBase;
import py.test.TestUtils;

public class VolumeMetadataMergerTest extends TestBase {
  @Test
  public void testNullMyVolume() throws IOException {
    long childVolumeId = 1L;
    int volumeOffset = 1;

    VolumeMetadata volume = customizeVolume(1, childVolumeId, volumeOffset);
    VolumeMetadataJsonMerger merger = new VolumeMetadataJsonMerger(null);
    merger.add(volume.toJsonString());

    assertTrue(merger.merge());

    VolumeMetadata myVolume = merger.getMyVolumeMetadata();

    assertEquals(volume.getChildVolumeId(), myVolume.getChildVolumeId());
    assertEquals(volume.getPositionOfFirstSegmentInLogicVolume(),
        myVolume.getPositionOfFirstSegmentInLogicVolume());
  }

  @Test
  public void testMergeStaleVolume() throws IOException {
    Long childVolumeId = 1L;
    int volumeOffset = 1;

    VolumeMetadata myVolume = customizeVolume(1, childVolumeId, volumeOffset);
    VolumeMetadataJsonMerger merger = new VolumeMetadataJsonMerger(myVolume.toJsonString());

    VolumeMetadata staleVolume = customizeVolume(0, null, 0);
    merger.add(staleVolume.toJsonString());

    assertFalse(merger.merge());
    myVolume = merger.getMyVolumeMetadata();
    assertEquals(childVolumeId, myVolume.getChildVolumeId());
    assertEquals(volumeOffset, myVolume.getPositionOfFirstSegmentInLogicVolume());
  }

  @Test
  public void testMergeStaleAndLatestVolume() throws IOException {
    VolumeMetadata myVolume = customizeVolume(1, null, 0);
    VolumeMetadataJsonMerger merger = new VolumeMetadataJsonMerger(myVolume.toJsonString());

    Long childVolumeId = 1L;
    int volumeOffset = 1;

    VolumeMetadata staleVolume = customizeVolume(0, null, 0);
    merger.add(staleVolume.toJsonString());
    VolumeMetadata newVolume = customizeVolume(2, childVolumeId, volumeOffset);
    merger.add(newVolume.toJsonString());

    assertTrue(merger.merge());
    myVolume = merger.getMyVolumeMetadata();

    assertEquals(childVolumeId, myVolume.getChildVolumeId());
    assertEquals(volumeOffset, myVolume.getPositionOfFirstSegmentInLogicVolume());
  }

  @Test
  public void testMergeInvalidVolumeJson() throws IOException {
    VolumeMetadata myVolume = customizeVolume(0, null, 0);
    VolumeMetadataJsonMerger merger = new VolumeMetadataJsonMerger(myVolume.toJsonString());

    Long childVolumeId = 1L;
    int volumeOffset = 1;

    merger.add("some string");
    VolumeMetadata newVolume = customizeVolume(1, childVolumeId, volumeOffset);
    merger.add(newVolume.toJsonString());

    assertTrue(merger.merge());
    myVolume = merger.getMyVolumeMetadata();

    assertEquals(childVolumeId, myVolume.getChildVolumeId());
    assertEquals(volumeOffset, myVolume.getPositionOfFirstSegmentInLogicVolume());
  }

  private VolumeMetadata customizeVolume(int version, Long childVolumeId, int volumeOffset) {
    String tagKey = "tagKey1111111111";
    String tagValue = "tagValue11111111";

    VolumeMetadata volume = TestUtils.generateVolumeMetadata();
    volume.setChildVolumeId(childVolumeId);
    volume.setPositionOfFirstSegmentInLogicVolume(volumeOffset);
    volume.setExtendingSize(Long.MAX_VALUE);
    volume.setAccountId(Long.MAX_VALUE);
    volume.setName("1111111111111111");
    volume.setRootVolumeId(Long.MAX_VALUE);
    volume.setVolumeSize(Long.MAX_VALUE);
    volume.setVersion(version);
    return volume;
  }
}
