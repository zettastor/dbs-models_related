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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;
import junit.framework.Assert;
import org.junit.Test;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentVersion;
import py.test.TestBase;
import py.test.TestUtils;

public class VolumeMetadataTest extends TestBase {
  @Test
  public void testSerializeDeserialize() throws IOException {
    long childVolumeId = Long.MAX_VALUE;
    int volumeOffset = Integer.MAX_VALUE;
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

    ObjectMapper mapper = new ObjectMapper();
    String volumeJson = mapper.writeValueAsString(volume);

    logger.debug(volumeJson);
    VolumeMetadata parsedVolume = mapper.readValue(volumeJson, VolumeMetadata.class);

    assertEquals(childVolumeId, parsedVolume.getChildVolumeId().longValue());
    assertEquals(volumeOffset, parsedVolume.getPositionOfFirstSegmentInLogicVolume());
    assertEquals(0, parsedVolume.getExtendingSize());
    assertEquals(volume.getAccountId(), parsedVolume.getAccountId());
    assertEquals(volume.getName(), parsedVolume.getName());
    assertEquals(volume.getRootVolumeId(), parsedVolume.getRootVolumeId());
    assertEquals(volume.getVolumeId(), parsedVolume.getVolumeId());
    assertEquals(volume.getVolumeType(), parsedVolume.getVolumeType());
  }

  @Test
  public void testUpdateStatusForSameVersion() {
    VolumeMetadata volumeMetadata = TestUtils.generateVolumeMetadata();
    volumeMetadata.setVolumeId(1);
    volumeMetadata.setSegmentSize(1);

    for (SegmentMetadata segment : volumeMetadata.getSegments()) {
      Iterator<SegmentUnitMetadata> segmentUnitIterator = segment.getSegmentUnitMetadataTable()
          .values().iterator();
      SegmentUnitMetadata segmentUnit = segmentUnitIterator.next();
      segmentUnit.setMembership(TestUtils.generateMembership(new SegmentVersion(1, 0), 1L, 2L, 3L));
      segmentUnit = segmentUnitIterator.next();
      segmentUnit.setMembership(TestUtils.generateMembership(new SegmentVersion(1, 0), 2L, 1L, 3L));
      segmentUnit = segmentUnitIterator.next();
      segmentUnit.setMembership(TestUtils.generateMembership(new SegmentVersion(1, 0), 2L, 1L, 3L));
    }

    VolumeStatus status = volumeMetadata.updateStatus();
    Assert.assertEquals(status, VolumeStatus.Unavailable);
  }
}
