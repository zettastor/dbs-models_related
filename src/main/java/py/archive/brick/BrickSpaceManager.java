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

package py.archive.brick;

import java.util.List;
import py.archive.page.MultiPageAddress;
import py.exception.NotEnoughSpaceException;

/**
 * Manager the space on a brick , there must be one and only one {@link BrickSpaceManager } on each
 * brick
 *
 * <p>Main responsibilities:
 *
 * <p>1. Allocate multi shadow page <br>
 *
 * <p>2. Free multi shadow page so that the freed space could be reused
 */
public interface BrickSpaceManager {
  /**
   * Get the free space on this brick. For instance, if the page size is 4K, and getFreeSpace()
   * returns 16K, then 4 pages can be allocated.
   */
  int getFreeSpace();

  /**
   * mark all pages has been used, and reformat brick space.
   */
  void markAllPageUsed();

  boolean isPageFree(int pageIndex);
}
