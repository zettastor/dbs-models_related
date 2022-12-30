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

package py.archive.page;

import py.archive.segment.SegId;
import py.storage.Storage;

/**
 * The address of a page, or the logic location of the page
 *
 * <p>It has 4 parts. The first part is the segment id, and other part is the offset of the page in
 * the segment
 *
 * <p>The implementation of a PageAddress has to implement its equals() and hashcode() functions!!!
 */
public interface PageAddress extends Comparable<PageAddress> {
  SegId getSegId();

  void setSegId(SegId segId);

  Storage getStorage();

  long getSegUnitOffsetInArchive();

  long getOffsetInSegment();

  long getPhysicalOffsetInArchive();

  long getLogicOffsetInSegment(int logicalPageSize);

  boolean isAdjacentTo(PageAddress pageAddress, int physicalPageSize);
}
