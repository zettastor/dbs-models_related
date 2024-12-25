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

package py.archive;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.test.TestBase;
import py.test.TestUtils;

public class SegmentUnitMetadataTest extends TestBase {
  @Test
  public void testSerializeSegmentUnit() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SegmentUnitMetadata metadata = new SegmentUnitMetadata(
        new SegId(Long.MAX_VALUE, Integer.MAX_VALUE), Long.MAX_VALUE);
    metadata.setStatus(SegmentUnitStatus.Deleting);
    metadata.setMembership(TestUtils.generateMembership());

    byte[] result1 = mapper.writeValueAsBytes(metadata);
    SegmentUnitMetadata parsedMetadata = mapper.readValue(result1, SegmentUnitMetadata.class);
    assertEquals(metadata, parsedMetadata);
  }
}