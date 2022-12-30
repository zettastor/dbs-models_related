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