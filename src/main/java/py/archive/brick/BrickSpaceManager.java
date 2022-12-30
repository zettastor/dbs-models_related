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
