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
