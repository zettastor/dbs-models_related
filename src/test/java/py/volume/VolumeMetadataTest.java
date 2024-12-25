/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
